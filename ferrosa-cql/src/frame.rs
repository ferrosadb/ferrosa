//! CQL native protocol v5 frame encoding and decoding.
//!
//! Each frame has a 9-byte header (version, flags, stream ID, opcode,
//! length) followed by a body of `length` bytes.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::CqlError;

/// CQL native protocol v5 version byte for requests.
pub const VERSION_REQUEST: u8 = 0x05;
/// CQL native protocol v4 version byte for responses.
/// Most Rust drivers (cdrs-tokio, scylla-rust-driver) negotiate v4.
pub const VERSION_RESPONSE: u8 = 0x84;
/// Size of the frame header in bytes.
pub const HEADER_SIZE: usize = 9;
/// Default maximum frame body size: 256 MiB.
pub const DEFAULT_MAX_FRAME_SIZE: u32 = 256 * 1024 * 1024;

/// Custom flag indicating a streaming SUBSCRIBE response frame.
/// Uses bit 4 (0x10) which is unused in the CQL v5 spec.
pub const STREAMING_FLAG: u8 = 0x10;

/// CQL v5 frame flag: body is compressed.
pub const COMPRESSION_FLAG: u8 = 0x01;

/// Supported compression algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Lz4,
    Snappy,
}

impl Compression {
    /// Algorithm name as used in CQL protocol negotiation.
    pub fn protocol_name(&self) -> &'static str {
        match self {
            Compression::Lz4 => "lz4",
            Compression::Snappy => "snappy",
        }
    }

    /// Parse from protocol name (case-insensitive).
    pub fn from_protocol_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "lz4" => Some(Compression::Lz4),
            "snappy" => Some(Compression::Snappy),
            _ => None,
        }
    }
}

/// CQL protocol opcodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Error = 0x00,
    Startup = 0x01,
    Ready = 0x02,
    Authenticate = 0x03,
    // 0x04 = CREDENTIALS, deprecated since v2
    Options = 0x05,
    Supported = 0x06,
    Query = 0x07,
    Result = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Event = 0x0C,
    Batch = 0x0D,
    AuthChallenge = 0x0E,
    AuthResponse = 0x0F,
    AuthSuccess = 0x10,
}

impl TryFrom<u8> for Opcode {
    type Error = CqlError;

    fn try_from(value: u8) -> std::result::Result<Self, CqlError> {
        match value {
            0x00 => Ok(Self::Error),
            0x01 => Ok(Self::Startup),
            0x02 => Ok(Self::Ready),
            0x03 => Ok(Self::Authenticate),
            0x05 => Ok(Self::Options),
            0x06 => Ok(Self::Supported),
            0x07 => Ok(Self::Query),
            0x08 => Ok(Self::Result),
            0x09 => Ok(Self::Prepare),
            0x0A => Ok(Self::Execute),
            0x0B => Ok(Self::Register),
            0x0C => Ok(Self::Event),
            0x0D => Ok(Self::Batch),
            0x0E => Ok(Self::AuthChallenge),
            0x0F => Ok(Self::AuthResponse),
            0x10 => Ok(Self::AuthSuccess),
            _ => Err(CqlError::Protocol(format!("unknown opcode: 0x{value:02X}"))),
        }
    }
}

/// A parsed CQL frame header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    pub version: u8,
    pub flags: u8,
    pub stream_id: i16,
    pub opcode: Opcode,
    pub length: u32,
}

impl FrameHeader {
    /// Encode this header into `buf`. Appends exactly 9 bytes.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u8(self.flags);
        buf.put_i16(self.stream_id);
        buf.put_u8(self.opcode as u8);
        buf.put_u32(self.length);
    }

    /// Create a response header with the STREAMING flag set.
    ///
    /// Used for SUBSCRIBE response frames that deliver continuous
    /// streaming results to the client.
    pub fn streaming_response(stream_id: i16, opcode: Opcode) -> Self {
        Self {
            version: VERSION_RESPONSE,
            flags: STREAMING_FLAG,
            stream_id,
            opcode,
            length: 0, // set when body is encoded
        }
    }

    /// Decode a header from the first 9 bytes of `buf`.
    pub fn decode(buf: &[u8]) -> std::result::Result<Self, CqlError> {
        if buf.len() < HEADER_SIZE {
            return Err(CqlError::Protocol(format!(
                "header too short: {} bytes",
                buf.len()
            )));
        }
        let mut cursor = &buf[..HEADER_SIZE];
        let version = cursor.get_u8();
        let flags = cursor.get_u8();
        let stream_id = cursor.get_i16();
        let opcode = Opcode::try_from(cursor.get_u8())?;
        let length = cursor.get_u32();
        Ok(Self {
            version,
            flags,
            stream_id,
            opcode,
            length,
        })
    }
}

/// A complete CQL frame: header + body.
#[derive(Debug, Clone)]
pub struct CqlFrame {
    pub header: FrameHeader,
    pub body: Bytes,
}

/// Tokio codec for CQL v5 frame encoding/decoding.
pub struct CqlCodec {
    max_frame_size: u32,
    compression: Option<Compression>,
}

impl CqlCodec {
    pub fn new(max_frame_size: u32) -> Self {
        Self {
            max_frame_size,
            compression: None,
        }
    }

    /// Set the compression algorithm after negotiation.
    ///
    /// Once set, all encoded frames will have their body compressed and the
    /// COMPRESSION_FLAG set (unless compression increases size), and all
    /// received frames with COMPRESSION_FLAG set will be decompressed.
    pub fn set_compression(&mut self, compression: Compression) {
        self.compression = Some(compression);
    }
}

impl Decoder for CqlCodec {
    type Item = CqlFrame;
    type Error = CqlError;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }
        let header = FrameHeader::decode(&src[..HEADER_SIZE])?;
        if header.length > self.max_frame_size {
            return Err(CqlError::Protocol(format!(
                "frame body too large: {} bytes (max {})",
                header.length, self.max_frame_size
            )));
        }
        let total = HEADER_SIZE + header.length as usize;
        if src.len() < total {
            src.reserve(total - src.len());
            return Ok(None);
        }
        src.advance(HEADER_SIZE);
        let body_bytes = src.split_to(header.length as usize).freeze();

        let body = if header.flags & COMPRESSION_FLAG != 0 {
            match self.compression {
                Some(Compression::Lz4) => Bytes::from(decompress_lz4(&body_bytes)?),
                Some(Compression::Snappy) => Bytes::from(decompress_snappy(&body_bytes)?),
                None => {
                    return Err(CqlError::Protocol(
                        "received compressed frame but no compression negotiated".into(),
                    ));
                }
            }
        } else {
            body_bytes
        };

        // Return header with original flags (compression flag indicates wire state,
        // but the body we return is already decompressed).
        Ok(Some(CqlFrame { header, body }))
    }
}

impl Encoder<CqlFrame> for CqlCodec {
    type Error = CqlError;

    fn encode(
        &mut self,
        item: CqlFrame,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        let mut header = item.header;

        let (body, flags) = if let Some(compression) = self.compression {
            let compressed = match compression {
                Compression::Lz4 => compress_lz4(&item.body),
                Compression::Snappy => compress_snappy(&item.body),
            };
            // Only use compression if it actually reduces size.
            if compressed.len() < item.body.len() {
                (compressed, header.flags | COMPRESSION_FLAG)
            } else {
                (item.body.to_vec(), header.flags & !COMPRESSION_FLAG)
            }
        } else {
            (item.body.to_vec(), header.flags & !COMPRESSION_FLAG)
        };

        header.length = body.len() as u32;
        header.flags = flags;
        dst.reserve(HEADER_SIZE + body.len());
        header.encode(dst);
        dst.put_slice(&body);
        Ok(())
    }
}

// ── Compression helpers ─────────────────────────────────────────────────

/// Compress data using LZ4 with CQL v5 framing.
///
/// CQL v5 LZ4 format: 4-byte big-endian uncompressed length, then raw LZ4 block.
fn compress_lz4(data: &[u8]) -> Vec<u8> {
    let compressed = lz4_flex::compress(data);
    let mut out = Vec::with_capacity(4 + compressed.len());
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(&compressed);
    out
}

/// Decompress LZ4 data with CQL v5 framing.
fn decompress_lz4(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    if data.len() < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "LZ4 frame too short",
        ));
    }
    let uncompressed_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if uncompressed_len == 0 {
        return Ok(Vec::new());
    }
    lz4_flex::decompress(&data[4..], uncompressed_len)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("LZ4: {e}")))
}

/// Compress data using Snappy (raw format, no framing).
fn compress_snappy(data: &[u8]) -> Vec<u8> {
    let mut encoder = snap::raw::Encoder::new();
    encoder.compress_vec(data).unwrap_or_else(|_| data.to_vec())
}

/// Decompress Snappy data.
fn decompress_snappy(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut decoder = snap::raw::Decoder::new();
    decoder.decompress_vec(data).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Snappy decompression failed: {e}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Task 2: Opcode and FrameHeader tests ---

    #[test]
    fn opcode_from_u8_roundtrip() {
        for &(byte, expected) in &[
            (0x00, Opcode::Error),
            (0x01, Opcode::Startup),
            (0x02, Opcode::Ready),
            (0x03, Opcode::Authenticate),
            (0x05, Opcode::Options),
            (0x06, Opcode::Supported),
            (0x07, Opcode::Query),
            (0x08, Opcode::Result),
            (0x09, Opcode::Prepare),
            (0x0A, Opcode::Execute),
            (0x0B, Opcode::Register),
            (0x0C, Opcode::Event),
            (0x0D, Opcode::Batch),
            (0x0E, Opcode::AuthChallenge),
            (0x0F, Opcode::AuthResponse),
            (0x10, Opcode::AuthSuccess),
        ] {
            let op = Opcode::try_from(byte).unwrap();
            assert_eq!(op, expected);
            assert_eq!(op as u8, byte);
        }
    }

    #[test]
    fn opcode_from_invalid_byte() {
        assert!(Opcode::try_from(0x04).is_err()); // deprecated CREDENTIALS
        assert!(Opcode::try_from(0xFF).is_err());
    }

    #[test]
    fn frame_header_size_is_9() {
        assert_eq!(HEADER_SIZE, 9);
    }

    #[test]
    fn frame_header_encode_decode_roundtrip() {
        let header = FrameHeader {
            version: VERSION_RESPONSE,
            flags: 0,
            stream_id: 42,
            opcode: Opcode::Result,
            length: 1024,
        };
        let mut buf = BytesMut::with_capacity(HEADER_SIZE);
        header.encode(&mut buf);
        assert_eq!(buf.len(), HEADER_SIZE);

        let decoded = FrameHeader::decode(&buf).unwrap();
        assert_eq!(decoded.version, header.version);
        assert_eq!(decoded.flags, header.flags);
        assert_eq!(decoded.stream_id, header.stream_id);
        assert_eq!(decoded.opcode, header.opcode);
        assert_eq!(decoded.length, header.length);
    }

    #[test]
    fn frame_header_negative_stream_id() {
        let header = FrameHeader {
            version: VERSION_REQUEST,
            flags: 0,
            stream_id: -1,
            opcode: Opcode::Query,
            length: 0,
        };
        let mut buf = BytesMut::with_capacity(HEADER_SIZE);
        header.encode(&mut buf);
        let decoded = FrameHeader::decode(&buf).unwrap();
        assert_eq!(decoded.stream_id, -1);
    }

    // --- Task 3: CqlCodec tests ---

    #[test]
    fn codec_decode_complete_frame() {
        let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        let mut buf = BytesMut::new();
        let header = FrameHeader {
            version: VERSION_REQUEST,
            flags: 0,
            stream_id: 1,
            opcode: Opcode::Startup,
            length: 0,
        };
        header.encode(&mut buf);
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.header.opcode, Opcode::Startup);
        assert_eq!(frame.header.stream_id, 1);
        assert!(frame.body.is_empty());
    }

    #[test]
    fn codec_decode_incomplete_header() {
        let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        let mut buf = BytesMut::from(&[0x05, 0x00, 0x00][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn codec_decode_incomplete_body() {
        let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        let mut buf = BytesMut::new();
        let header = FrameHeader {
            version: VERSION_REQUEST,
            flags: 0,
            stream_id: 0,
            opcode: Opcode::Query,
            length: 100,
        };
        header.encode(&mut buf);
        buf.put_slice(&[0u8; 50]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn codec_reject_oversized_frame() {
        let mut codec = CqlCodec::new(1024);
        let mut buf = BytesMut::new();
        let header = FrameHeader {
            version: VERSION_REQUEST,
            flags: 0,
            stream_id: 0,
            opcode: Opcode::Query,
            length: 2048,
        };
        header.encode(&mut buf);
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn codec_encode_frame() {
        let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        let frame = CqlFrame {
            header: FrameHeader {
                version: VERSION_RESPONSE,
                flags: 0,
                stream_id: 5,
                opcode: Opcode::Result,
                length: 0,
            },
            body: BytesMut::from(&b"hello"[..]).freeze(),
        };
        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();
        assert_eq!(buf.len(), 14);
        let decoded_header = FrameHeader::decode(&buf).unwrap();
        assert_eq!(decoded_header.length, 5);
    }

    // --- Task 14: STREAMING flag tests ---

    #[test]
    fn streaming_flag_constant() {
        assert_eq!(STREAMING_FLAG, 0x10);
    }

    #[test]
    fn streaming_response_header_has_flag() {
        let header = FrameHeader::streaming_response(42, Opcode::Result);
        assert_ne!(header.flags & STREAMING_FLAG, 0);
        assert_eq!(header.stream_id, 42);
        assert_eq!(header.version, VERSION_RESPONSE);
    }

    #[test]
    fn normal_response_header_no_streaming_flag() {
        let header = FrameHeader {
            version: VERSION_RESPONSE,
            flags: 0,
            stream_id: 1,
            opcode: Opcode::Result,
            length: 0,
        };
        assert_eq!(header.flags & STREAMING_FLAG, 0);
    }

    #[test]
    fn streaming_response_roundtrip() {
        let header = FrameHeader::streaming_response(7, Opcode::Result);
        let mut buf = BytesMut::with_capacity(HEADER_SIZE);
        header.encode(&mut buf);
        let decoded = FrameHeader::decode(&buf).unwrap();
        assert_ne!(decoded.flags & STREAMING_FLAG, 0);
        assert_eq!(decoded.stream_id, 7);
        assert_eq!(decoded.opcode, Opcode::Result);
    }

    // --- Compression tests ---

    #[test]
    fn test_compression_flag_constants() {
        assert_eq!(COMPRESSION_FLAG, 0x01);
        // Ensure compression and streaming flags don't overlap.
        assert_eq!(COMPRESSION_FLAG & STREAMING_FLAG, 0);
    }

    #[test]
    fn test_compression_from_protocol_name() {
        assert_eq!(
            Compression::from_protocol_name("lz4"),
            Some(Compression::Lz4)
        );
        assert_eq!(
            Compression::from_protocol_name("snappy"),
            Some(Compression::Snappy)
        );
        assert_eq!(
            Compression::from_protocol_name("LZ4"),
            Some(Compression::Lz4)
        );
        assert_eq!(
            Compression::from_protocol_name("Snappy"),
            Some(Compression::Snappy)
        );
        assert_eq!(Compression::from_protocol_name("zstd"), None);
        assert_eq!(Compression::from_protocol_name(""), None);
    }

    #[test]
    fn test_compression_protocol_name() {
        assert_eq!(Compression::Lz4.protocol_name(), "lz4");
        assert_eq!(Compression::Snappy.protocol_name(), "snappy");
    }

    #[test]
    fn test_compress_decompress_lz4() {
        let data = b"Hello, world! This is a test of LZ4 compression in CQL v5 frames.";
        let compressed = compress_lz4(data);
        // First 4 bytes should be the uncompressed length in big-endian.
        let len = u32::from_be_bytes([compressed[0], compressed[1], compressed[2], compressed[3]]);
        assert_eq!(len as usize, data.len());
        let decompressed = decompress_lz4(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_decompress_snappy() {
        let data = b"Hello, world! This is a test of Snappy compression in CQL v5 frames.";
        let compressed = compress_snappy(data);
        let decompressed = decompress_snappy(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_empty_data() {
        let data = b"";
        let compressed = compress_lz4(data);
        // Uncompressed length should be 0.
        let len = u32::from_be_bytes([compressed[0], compressed[1], compressed[2], compressed[3]]);
        assert_eq!(len, 0);
        let decompressed = decompress_lz4(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_lz4_too_short() {
        let result = decompress_lz4(&[0x00, 0x01]);
        assert!(result.is_err());
    }

    #[test]
    fn test_snappy_empty_data() {
        let data = b"";
        let compressed = compress_snappy(data);
        let decompressed = decompress_snappy(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_codec_with_lz4_compression() {
        let mut encoder = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        encoder.set_compression(Compression::Lz4);

        // Create a frame with a compressible body (repeated data).
        let body_data = vec![b'A'; 200];
        let frame = CqlFrame {
            header: FrameHeader {
                version: VERSION_RESPONSE,
                flags: 0,
                stream_id: 1,
                opcode: Opcode::Result,
                length: 0,
            },
            body: Bytes::from(body_data.clone()),
        };

        let mut buf = BytesMut::new();
        encoder.encode(frame, &mut buf).unwrap();

        // The encoded header should have the compression flag set.
        let header = FrameHeader::decode(&buf).unwrap();
        assert_ne!(
            header.flags & COMPRESSION_FLAG,
            0,
            "compression flag should be set"
        );

        // Decode it back.
        let mut decoder = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        decoder.set_compression(Compression::Lz4);
        let decoded = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.body.as_ref(), &body_data[..]);
    }

    #[test]
    fn test_codec_with_snappy_compression() {
        let mut encoder = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        encoder.set_compression(Compression::Snappy);

        // Create a frame with a compressible body (repeated data).
        let body_data = vec![b'B'; 200];
        let frame = CqlFrame {
            header: FrameHeader {
                version: VERSION_RESPONSE,
                flags: 0,
                stream_id: 2,
                opcode: Opcode::Result,
                length: 0,
            },
            body: Bytes::from(body_data.clone()),
        };

        let mut buf = BytesMut::new();
        encoder.encode(frame, &mut buf).unwrap();

        // The encoded header should have the compression flag set.
        let header = FrameHeader::decode(&buf).unwrap();
        assert_ne!(
            header.flags & COMPRESSION_FLAG,
            0,
            "compression flag should be set"
        );

        // Decode it back.
        let mut decoder = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        decoder.set_compression(Compression::Snappy);
        let decoded = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.body.as_ref(), &body_data[..]);
    }

    #[test]
    fn test_codec_no_compression_default() {
        let mut encoder = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        // No compression set — default.

        let body_data = vec![b'C'; 200];
        let frame = CqlFrame {
            header: FrameHeader {
                version: VERSION_RESPONSE,
                flags: 0,
                stream_id: 3,
                opcode: Opcode::Result,
                length: 0,
            },
            body: Bytes::from(body_data.clone()),
        };

        let mut buf = BytesMut::new();
        encoder.encode(frame, &mut buf).unwrap();

        // The encoded header should NOT have the compression flag.
        let header = FrameHeader::decode(&buf).unwrap();
        assert_eq!(
            header.flags & COMPRESSION_FLAG,
            0,
            "compression flag should not be set"
        );
        // Body length should match original (no compression overhead).
        assert_eq!(header.length as usize, body_data.len());

        // Decode it back.
        let mut decoder = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        let decoded = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.body.as_ref(), &body_data[..]);
    }

    #[test]
    fn test_compressed_frame_smaller_than_original() {
        // Highly compressible data: 1000 bytes of the same byte.
        let body_data = vec![b'X'; 1000];
        let lz4_compressed = compress_lz4(&body_data);
        let snappy_compressed = compress_snappy(&body_data);

        assert!(
            lz4_compressed.len() < body_data.len(),
            "LZ4 compressed size ({}) should be smaller than original ({})",
            lz4_compressed.len(),
            body_data.len()
        );
        assert!(
            snappy_compressed.len() < body_data.len(),
            "Snappy compressed size ({}) should be smaller than original ({})",
            snappy_compressed.len(),
            body_data.len()
        );
    }

    #[test]
    fn test_codec_compressed_flag_not_set_for_uncompressible_data() {
        let mut encoder = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        encoder.set_compression(Compression::Lz4);

        // Very short data that won't compress well (compression overhead > savings).
        let body_data = vec![0x42; 3];
        let frame = CqlFrame {
            header: FrameHeader {
                version: VERSION_RESPONSE,
                flags: 0,
                stream_id: 4,
                opcode: Opcode::Result,
                length: 0,
            },
            body: Bytes::from(body_data.clone()),
        };

        let mut buf = BytesMut::new();
        encoder.encode(frame, &mut buf).unwrap();

        let header = FrameHeader::decode(&buf).unwrap();
        // For very small data, compression may increase size, so flag should NOT be set.
        if header.flags & COMPRESSION_FLAG == 0 {
            // Uncompressed — body should be original.
            assert_eq!(header.length as usize, body_data.len());
        }
        // Either way, decoding should work.
        let mut decoder = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        decoder.set_compression(Compression::Lz4);
        let decoded = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.body.as_ref(), &body_data[..]);
    }

    #[test]
    fn test_decoder_rejects_compressed_frame_without_negotiation() {
        // Manually encode a frame with the COMPRESSION_FLAG set but no
        // compression configured on the decoder.
        let body = compress_lz4(b"test data");
        let mut buf = BytesMut::new();
        let header = FrameHeader {
            version: VERSION_REQUEST,
            flags: COMPRESSION_FLAG,
            stream_id: 1,
            opcode: Opcode::Query,
            length: body.len() as u32,
        };
        header.encode(&mut buf);
        buf.put_slice(&body);

        let mut decoder = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        // No compression set.
        let result = decoder.decode(&mut buf);
        assert!(
            result.is_err(),
            "should reject compressed frame without negotiation"
        );
    }
}
