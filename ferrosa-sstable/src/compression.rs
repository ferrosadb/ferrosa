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

use std::cell::RefCell;

use ferrosa_common::{Error, Result};
use zstd::stream::raw::Operation;
use zstd::zstd_safe::{InBuffer, OutBuffer};

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

thread_local! {
    /// A per-thread, lazily-created zstd compression context, reused across
    /// `compress_into` calls at the same level.
    ///
    /// `compress_into` drives `zstd_safe::CCtx` directly through the same
    /// `compress_stream`/`end_stream` calls `zstd::encode_all` makes
    /// underneath its `Write`-based `Encoder` (see the comment in
    /// `compress_into` below for why the higher-level wrappers — both the
    /// streaming `Encoder` and the one-shot `bulk::Compressor` — don't fit).
    /// Reusing one `CCtx` per thread here, instead of creating one inside
    /// `compress_into`, is what makes the pump's chunk-compression loop
    /// allocation-free after its first call (T-036, bounded-ring rule): the
    /// `compression_pool()` rayon threads each warm their own slot once and
    /// reuse it for every later chunk. It is only rebuilt when the
    /// requested level changes, which does not happen mid-table.
    static ZSTD_CCTX: RefCell<Option<(i32, zstd::zstd_safe::CCtx<'static>)>> =
        const { RefCell::new(None) };
}

/// Converts a `zstd_safe` error code into a `ferrosa_common::Error`.
fn zstd_error(code: usize) -> Error {
    Error::Io(std::io::Error::other(zstd::zstd_safe::get_error_name(code)))
}

impl Compression {
    /// Default chunk size matching Cassandra's DEFAULT_CHUNK_LENGTH.
    pub const DEFAULT_CHUNK_SIZE: usize = 16384;

    /// Worst-case number of bytes `compress_into` can write for `len` input
    /// bytes.
    ///
    /// Callers (the write pump's `ChunkCompressor`, T-038) preallocate `dst`
    /// at this size once, at open, and reuse it for every chunk — the bound
    /// must never depend on anything but `len`.
    pub fn compress_bound(&self, len: usize) -> usize {
        match self {
            Compression::None => len,
            Compression::Lz4 => {
                // `compress_into` below prepends a 4-byte little-endian
                // uncompressed length, matching `compress_prepend_size`'s
                // on-disk format (`lz4_flex::decompress_size_prepended`
                // expects exactly this layout).
                4 + lz4_flex::block::get_maximum_output_size(len)
            }
            Compression::Zstd { .. } => zstd::zstd_safe::compress_bound(len),
        }
    }

    /// Compress `src` into caller-owned `dst`, writing no more than
    /// `dst.len()` bytes, and returning the number of bytes written.
    ///
    /// `dst` must be at least `compress_bound(src.len())` bytes; if it is
    /// smaller this returns `Err` without writing anything into `dst` —
    /// never a truncated payload, and never a panic.
    ///
    /// The bytes written are byte-identical to `compress(src)` for every
    /// codec (proved by the `compress_into_matches_compress_*` tests), so a
    /// reader written against `decompress` cannot tell which path produced
    /// the file.
    pub fn compress_into(&self, src: &[u8], dst: &mut [u8]) -> Result<usize> {
        let bound = self.compress_bound(src.len());
        if dst.len() < bound {
            return Err(Error::InvalidData(format!(
                "compress_into: dst is {} bytes, need at least {bound} for {} input bytes",
                dst.len(),
                src.len()
            )));
        }
        match self {
            Compression::None => {
                dst[..src.len()].copy_from_slice(src);
                Ok(src.len())
            }
            Compression::Lz4 => {
                dst[..4].copy_from_slice(&(src.len() as u32).to_le_bytes());
                let written = lz4_flex::block::compress_into(src, &mut dst[4..])
                    .map_err(|e| Error::InvalidData(format!("LZ4 compression failed: {e}")))?;
                Ok(4 + written)
            }
            Compression::Zstd { level } => {
                // `zstd::bulk::Compressor::compress_to_buffer`
                // (`ZSTD_compress2`) looks like the obvious into-buffer API
                // here, but it always hands zstd the *whole* input in one
                // call with immediate `ZSTD_e_end`. zstd auto-pledges the
                // exact input length for that pattern and picks a
                // size-tuned frame header and window, which differs from
                // what `compress` (`zstd::encode_all`) produces: it streams
                // through an `io::Write`, so its `write`/`finish` calls
                // never hand zstd the full, known-length buffer in a single
                // `e_end` call, and it never pledges a size. The two paths
                // are not byte-identical (confirmed empirically: the bulk
                // API's frame header differs by input size). The streaming
                // `write::Encoder` wrapper does match `compress` exactly,
                // since it is what `compress` itself uses — but it owns a
                // 32 KiB `Vec` that it allocates fresh per encoder, with no
                // way to reclaim and reuse that allocation across calls.
                //
                // So: drive `zstd_safe::CCtx` with the same two calls
                // `encode_all` makes below it — `compress_stream`
                // (`ZSTD_e_continue`) until `src` is consumed, then
                // `end_stream` (`ZSTD_e_end`) until the frame is closed —
                // straight into the caller's `dst`. This is the same
                // sequence, on the same reused context, with no
                // intermediate buffer at all, so it is both allocation-free
                // after warm-up and byte-identical to `compress`
                // (`compress_into_matches_compress_zstd`).
                let level = *level;
                ZSTD_CCTX.with(|cell| {
                    let mut slot = cell.borrow_mut();
                    let needs_new = !matches!(&*slot, Some((cached, _)) if *cached == level);
                    if needs_new {
                        let mut cctx = zstd::zstd_safe::CCtx::create();
                        cctx.set_parameter(zstd::zstd_safe::CParameter::CompressionLevel(level))
                            .map_err(zstd_error)?;
                        *slot = Some((level, cctx));
                    }
                    let (_, cctx) = slot
                        .as_mut()
                        .expect("populated by the `needs_new` branch above");

                    let mut encoder = zstd::stream::raw::Encoder::with_context(cctx);
                    let mut input = InBuffer::around(src);
                    let mut output = OutBuffer::around(dst);

                    // `Operation::run` (`compress_stream`) is not required
                    // to consume all of `input` in one call even when
                    // `output` has room, so loop until it has.
                    while input.pos < input.src.len() {
                        encoder.run(&mut input, &mut output).map_err(Error::Io)?;
                    }
                    // `finish` (`end_stream`) returns how many bytes are
                    // still owed (footer/checksum); keep calling until 0.
                    loop {
                        let remaining = encoder.finish(&mut output, true).map_err(Error::Io)?;
                        if remaining == 0 {
                            break;
                        }
                    }

                    Ok(output.pos())
                })
            }
        }
    }

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

    // --- T-036: compress_into ---

    use proptest::prelude::*;

    /// Deterministic, dependency-free "random" bytes: a xorshift64 stream
    /// keyed by `seed`, not a real PRNG's worth of randomness but enough to
    /// avoid compressing trivially to nothing.
    fn pseudo_random_bytes(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed | 1;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state & 0xff) as u8
            })
            .collect()
    }

    /// 64 KiB of alternating 4 KiB runs of pseudo-random bytes and zeros —
    /// the "mixed" fixed case the packet's test list calls for.
    fn mixed_64kib() -> Vec<u8> {
        let mut buf = pseudo_random_bytes(64 * 1024, 0xF00D);
        for chunk in buf.chunks_mut(4096).step_by(2) {
            chunk.fill(0);
        }
        buf
    }

    fn fixed_cases() -> Vec<Vec<u8>> {
        vec![
            Vec::new(),
            vec![0x42],
            pseudo_random_bytes(16 * 1024, 0xC0FFEE),
            vec![0u8; 16 * 1024],
            mixed_64kib(),
        ]
    }

    /// `compress_into` must write exactly what `compress` returns, into a
    /// `compress_bound`-sized buffer, and the result must still decompress
    /// to the original input.
    fn assert_compress_into_matches_compress(compression: &Compression, data: &[u8]) {
        let via_compress = compression
            .compress(data)
            .unwrap_or_else(|e| panic!("{compression:?}: compress failed: {e:?}"));

        let bound = compression.compress_bound(data.len());
        assert!(
            bound >= via_compress.len(),
            "{compression:?}: compress_bound({}) = {bound} must cover compress()'s {} bytes",
            data.len(),
            via_compress.len()
        );

        let mut dst = vec![0u8; bound];
        let written = compression
            .compress_into(data, &mut dst)
            .unwrap_or_else(|e| panic!("{compression:?}: compress_into failed: {e:?}"));

        assert_eq!(
            &dst[..written],
            &via_compress[..],
            "{compression:?}: compress_into must produce byte-identical output to compress"
        );

        let decompressed = compression
            .decompress(&dst[..written], data.len())
            .unwrap_or_else(|e| panic!("{compression:?}: decompress failed: {e:?}"));
        assert_eq!(
            decompressed, data,
            "{compression:?}: decompress(compress_into(x)) must equal x"
        );
    }

    #[test]
    fn compress_into_matches_compress_none() {
        for data in fixed_cases() {
            assert_compress_into_matches_compress(&Compression::None, &data);
        }
    }

    #[test]
    fn compress_into_matches_compress_lz4() {
        for data in fixed_cases() {
            assert_compress_into_matches_compress(&Compression::Lz4, &data);
        }
    }

    #[test]
    fn compress_into_matches_compress_zstd() {
        for data in fixed_cases() {
            assert_compress_into_matches_compress(&Compression::Zstd { level: 3 }, &data);
        }
    }

    #[test]
    fn compress_into_dst_too_small_errs() {
        let data = pseudo_random_bytes(4096, 0xABCD);
        for compression in [
            Compression::None,
            Compression::Lz4,
            Compression::Zstd { level: 3 },
        ] {
            let bound = compression.compress_bound(data.len());
            let mut dst = vec![0u8; bound - 1];
            let result = compression.compress_into(&data, &mut dst);
            assert!(
                result.is_err(),
                "{compression:?}: dst one byte short of compress_bound must error, not truncate"
            );
            // Never a truncated payload: on error the destination must be
            // left exactly as it was passed in (all zeros here), never
            // partially written.
            assert!(
                dst.iter().all(|&b| b == 0),
                "{compression:?}: a failed compress_into must not have written into dst"
            );
        }
    }

    proptest! {
        #[test]
        fn compress_into_matches_compress_prop_none(len in 0usize..=65536) {
            let data = pseudo_random_bytes(len, (len as u64) ^ 0x51);
            assert_compress_into_matches_compress(&Compression::None, &data);
        }

        #[test]
        fn compress_into_matches_compress_prop_lz4(len in 0usize..=65536) {
            let data = pseudo_random_bytes(len, (len as u64) ^ 0x52);
            assert_compress_into_matches_compress(&Compression::Lz4, &data);
        }

        #[test]
        fn compress_into_matches_compress_prop_zstd(len in 0usize..=65536) {
            let data = pseudo_random_bytes(len, (len as u64) ^ 0x53);
            assert_compress_into_matches_compress(&Compression::Zstd { level: 3 }, &data);
        }
    }
}
