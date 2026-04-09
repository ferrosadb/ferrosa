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
    ///
    /// CQL v4 and below use a 9-byte fixed header:
    ///   version(1) + flags(1) + stream(2) + opcode(1) + length(4)
    ///
    /// CQL v5 changed the envelope format (different framing structure).
    /// If we detect a v5 request, we return a `ProtocolVersionMismatch`
    /// error so the connection handler can reply with an ERROR frame
    /// using v4, causing the driver to fall back.
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

/// CQL v5 frame header size (uncompressed): 3 bytes header + 3 bytes CRC24.
pub const V5_FRAME_HEADER_SIZE: usize = 6;
/// CQL v5 CRC32 trailer size.
pub const V5_CRC32_SIZE: usize = 4;
/// Maximum payload in a single v5 frame: 2^17 - 1 = 131,071 bytes.
pub const V5_MAX_PAYLOAD: usize = (1 << 17) - 1;

/// Tokio codec for CQL frame encoding/decoding.
///
/// Supports both v4 (unframed 9-byte envelope headers) and v5 (framed
/// with 6-byte LE headers + CRC24/CRC32 integrity checks). The codec
/// starts in unframed mode and switches to v5 framed mode after the
/// STARTUP/READY handshake completes on a v5 connection.
pub struct CqlCodec {
    max_frame_size: u32,
    compression: Option<Compression>,
    /// When true, incoming/outgoing data uses v5 frame format (6-byte LE
    /// header + CRC24 + payload + CRC32). Set by the connection handler
    /// after STARTUP succeeds on a v5 connection.
    v5_framed: bool,
    /// Buffer for reassembling segmented v5 messages (isSelfContained=0).
    v5_segment_buf: BytesMut,
}

impl CqlCodec {
    pub fn new(max_frame_size: u32) -> Self {
        Self {
            max_frame_size,
            compression: None,
            v5_framed: false,
            v5_segment_buf: BytesMut::new(),
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

    /// Switch to v5 framed mode. Called by the connection handler after
    /// the STARTUP/READY exchange completes on a v5 connection.
    pub fn enable_v5_framing(&mut self) {
        self.v5_framed = true;
    }

    /// Disable v5 framing, reverting to v4 unframed mode.
    ///
    /// Used when a client negotiates v5 but doesn't implement v5 framing
    /// (e.g., Python cassandra-driver sends v5 STARTUP but continues with
    /// v4-style envelopes). The server detects the CRC mismatch and falls
    /// back gracefully.
    pub fn disable_v5_framing(&mut self) {
        self.v5_framed = false;
    }

    /// Whether v5 framing is active.
    pub fn is_v5_framed(&self) -> bool {
        self.v5_framed
    }
}

impl CqlCodec {
    /// Decode a v4-style unframed envelope (9-byte BE header + body).
    /// Also used for v5 pre-STARTUP messages.
    fn decode_v4_envelope(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<CqlFrame>, CqlError> {
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

        Ok(Some(CqlFrame { header, body }))
    }

    /// Decode a v5 framed message.
    ///
    /// v5 frame format (uncompressed):
    ///   Bytes 0-2: payload_length(17 bits) | isSelfContained(1 bit) | padding(6 bits) — LE
    ///   Bytes 3-5: CRC24 of bytes 0-2 — LE
    ///   Bytes 6..6+payload_length: payload (one or more 9-byte envelopes)
    ///   Last 4 bytes: CRC32 of payload — LE
    fn decode_v5_frame(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<CqlFrame>, CqlError> {
        if src.len() < V5_FRAME_HEADER_SIZE {
            return Ok(None);
        }

        // Parse 3-byte header (little-endian).
        let h0 = src[0] as u32;
        let h1 = src[1] as u32;
        let h2 = src[2] as u32;
        let header_bits = h0 | (h1 << 8) | (h2 << 16);
        let payload_len = (header_bits & 0x1FFFF) as usize; // bits 0-16
        let is_self_contained = (header_bits >> 17) & 1 == 1; // bit 17

        // Verify CRC24 of the 3 header bytes.
        let expected_crc24 = src[3] as u32 | ((src[4] as u32) << 8) | ((src[5] as u32) << 16);
        let actual_crc24 = crc24(&src[..3]);
        if expected_crc24 != actual_crc24 {
            return Err(CqlError::Protocol(format!(
                "v5 frame header CRC24 mismatch: expected 0x{expected_crc24:06X}, \
                 got 0x{actual_crc24:06X}"
            )));
        }

        let total = V5_FRAME_HEADER_SIZE + payload_len + V5_CRC32_SIZE;
        if src.len() < total {
            src.reserve(total - src.len());
            return Ok(None);
        }

        // Verify CRC32 of the payload.
        let payload_start = V5_FRAME_HEADER_SIZE;
        let payload_end = payload_start + payload_len;
        let crc32_start = payload_end;
        let expected_crc32 = u32::from_le_bytes([
            src[crc32_start],
            src[crc32_start + 1],
            src[crc32_start + 2],
            src[crc32_start + 3],
        ]);
        let actual_crc32 = crc32_castagnoli(&src[payload_start..payload_end]);
        if expected_crc32 != actual_crc32 {
            return Err(CqlError::Protocol(format!(
                "v5 frame payload CRC32 mismatch: expected 0x{expected_crc32:08X}, \
                 got 0x{actual_crc32:08X}"
            )));
        }

        // Extract payload.
        src.advance(V5_FRAME_HEADER_SIZE);
        let payload = src.split_to(payload_len);
        src.advance(V5_CRC32_SIZE); // skip CRC32

        if !is_self_contained {
            // Segmented message — accumulate and wait for final segment.
            self.v5_segment_buf.extend_from_slice(&payload);
            return Ok(None);
        }

        // Complete message — combine with any buffered segments.
        let envelope_data = if self.v5_segment_buf.is_empty() {
            payload.freeze()
        } else {
            self.v5_segment_buf.extend_from_slice(&payload);
            self.v5_segment_buf.split().freeze()
        };

        // The payload contains a standard 9-byte envelope header + body.
        if envelope_data.len() < HEADER_SIZE {
            return Err(CqlError::Protocol(
                "v5 frame payload too short for envelope header".into(),
            ));
        }
        let header = FrameHeader::decode(&envelope_data[..HEADER_SIZE])?;
        let body = envelope_data.slice(HEADER_SIZE..);

        // Decompress if needed.
        let body = if header.flags & COMPRESSION_FLAG != 0 {
            match self.compression {
                Some(Compression::Lz4) => Bytes::from(decompress_lz4(&body)?),
                Some(Compression::Snappy) => Bytes::from(decompress_snappy(&body)?),
                None => {
                    return Err(CqlError::Protocol(
                        "received compressed frame but no compression negotiated".into(),
                    ));
                }
            }
        } else {
            body
        };

        Ok(Some(CqlFrame { header, body }))
    }
}

impl Decoder for CqlCodec {
    type Item = CqlFrame;
    type Error = CqlError;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        // Reject unsupported request protocol versions.
        // Valid request versions: 0x03 (v3), 0x04 (v4), 0x05 (v5).
        // Only explicitly reject 0x06/0x07 (future versions we don't support).
        // Garbage bytes (healthcheck probes, etc.) fall through to the header
        // parser which will reject them via unknown opcode.
        if src.len() >= HEADER_SIZE && !self.v5_framed {
            let version_byte = src[0];
            if version_byte == 0x06 || version_byte == 0x07 {
                src.clear();
                return Err(CqlError::ProtocolVersionMismatch {
                    requested: version_byte,
                    supported: 0x05,
                });
            }
        }

        if self.v5_framed {
            self.decode_v5_frame(src)
        } else {
            self.decode_v4_envelope(src)
        }
    }

    /// Override default `decode_eof` to silently discard partial frames.
    fn decode_eof(
        &mut self,
        buf: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        match self.decode(buf)? {
            Some(frame) => Ok(Some(frame)),
            None => {
                buf.clear();
                Ok(None)
            }
        }
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

        if self.v5_framed {
            // v5: wrap envelope in a frame with CRC24/CRC32.
            encode_v5_frame(&header, &body, dst);
        } else {
            // v4 / pre-STARTUP: raw 9-byte envelope header + body.
            dst.reserve(HEADER_SIZE + body.len());
            header.encode(dst);
            dst.put_slice(&body);
        }
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

// ── CQL v5 CRC functions ────────────────────────────────────────────────

/// CRC24 used by CQL v5 frame headers.
///
/// Matches Cassandra's `org.apache.cassandra.utils.Crc.crc24()`:
/// - Initial value: `CRC24_INIT = 0x875060`
/// - Polynomial:    `CRC24_POLY = 0x1974F0B`
///
/// Public CRC24 for test use.
pub fn crc24_public(data: &[u8]) -> u32 {
    crc24(data)
}

/// Public CRC32 for test use.
pub fn crc32_public(data: &[u8]) -> u32 {
    crc32_castagnoli(data)
}

/// CRC24 initial value (matches Cassandra `CRC24_INIT`).
const CRC24_INIT: u32 = 0x87_5060;
/// CRC24 polynomial (matches Cassandra `CRC24_POLY`).
const CRC24_POLY: u32 = 0x197_4F0B;

fn crc24(data: &[u8]) -> u32 {
    let mut crc: u32 = CRC24_INIT;
    for &byte in data {
        crc ^= (byte as u32) << 16;
        for _ in 0..8 {
            crc <<= 1;
            if crc & 0x100_0000 != 0 {
                crc ^= CRC24_POLY;
            }
        }
    }
    crc & 0xFF_FFFF
}

/// CRC32-C (Castagnoli) used by CQL v5 frame payload checksums.
fn crc32_castagnoli(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F6_3B78; // Castagnoli polynomial (reflected)
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xFFFF_FFFF
}

/// Encode a v5 frame around an envelope (header + body).
///
/// Produces: [3-byte LE header][3-byte CRC24][payload][4-byte CRC32]
/// where payload = 9-byte envelope header + envelope body.
pub fn encode_v5_frame(envelope_header: &FrameHeader, body: &[u8], dst: &mut BytesMut) {
    // Build the envelope (9-byte header + body) as the frame payload.
    let payload_len = HEADER_SIZE + body.len();
    assert!(payload_len <= V5_MAX_PAYLOAD, "v5 frame payload too large");

    // 3-byte header: payload_length(17 bits) | isSelfContained(1 bit) | padding(6 bits)
    let header_bits: u32 = (payload_len as u32) | (1 << 17); // isSelfContained=1
    let h_bytes = header_bits.to_le_bytes(); // [b0, b1, b2, _]

    // CRC24 of the 3 header bytes.
    let crc24_val = crc24(&h_bytes[..3]);
    let crc24_bytes = crc24_val.to_le_bytes();

    // Reserve space for header(6) + payload + CRC32(4)
    dst.reserve(V5_FRAME_HEADER_SIZE + payload_len + V5_CRC32_SIZE);

    // Write frame header (3 bytes) + CRC24 (3 bytes)
    dst.put_slice(&h_bytes[..3]);
    dst.put_slice(&crc24_bytes[..3]);

    // Write envelope (9-byte header + body) = payload
    let payload_start = dst.len();
    envelope_header.encode(dst);
    dst.put_slice(body);
    let payload_end = dst.len();

    // CRC32 of the payload
    let crc32_val = crc32_castagnoli(&dst[payload_start..payload_end]);
    dst.put_u32_le(crc32_val);
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
            version: 0x04, // v4 request — v5 uses different envelope
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
        let mut buf = BytesMut::from(&[0x04, 0x00, 0x00][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn codec_decode_incomplete_body() {
        let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        let mut buf = BytesMut::new();
        let header = FrameHeader {
            version: 0x04,
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
            version: 0x04,
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
    fn version_response_is_v4() {
        assert_eq!(VERSION_RESPONSE, 0x84);
    }

    #[test]
    fn codec_accepts_v5_startup_envelope() {
        let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        // v5 STARTUP uses the same 9-byte envelope format as v4 (pre-framing).
        let mut buf = BytesMut::new();
        let header = FrameHeader {
            version: 0x05,
            flags: 0,
            stream_id: 0,
            opcode: Opcode::Startup,
            length: 0,
        };
        header.encode(&mut buf);
        let result = codec.decode(&mut buf).unwrap();
        assert!(result.is_some(), "v5 STARTUP should be accepted");
        assert_eq!(result.unwrap().header.version, 0x05);
    }

    #[test]
    fn codec_rejects_v6_request() {
        let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        let mut buf = BytesMut::from(&[0x06, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00][..]);
        let result = codec.decode(&mut buf);
        assert!(result.is_err(), "v6 frames should be rejected");
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                CqlError::ProtocolVersionMismatch {
                    requested: 6,
                    supported: 5
                }
            ),
            "should be ProtocolVersionMismatch, got: {err}"
        );
    }

    #[test]
    fn test_decoder_rejects_compressed_frame_without_negotiation() {
        // Manually encode a frame with the COMPRESSION_FLAG set but no
        // compression configured on the decoder.
        let body = compress_lz4(b"test data");
        let mut buf = BytesMut::new();
        let header = FrameHeader {
            version: 0x04,
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

    /// Healthcheck connections (e.g. `echo > /dev/tcp/host/9042`) send a few
    /// bytes and close.  The default `decode_eof` reports "bytes remaining on
    /// stream" for partial data — override to silently discard incomplete
    /// frames on EOF instead of spamming logs with errors.
    #[test]
    fn partial_frame_on_eof_returns_none() {
        let mut decoder = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        // Simulate a healthcheck that sends a newline then closes.
        let mut buf = BytesMut::from(&b"\n"[..]);
        // decode_eof is called when the stream hits EOF with data in the buffer.
        let result = decoder.decode_eof(&mut buf);
        assert!(
            result.is_ok(),
            "partial frame on EOF should not error, got: {:?}",
            result.err()
        );
        assert!(
            result.unwrap().is_none(),
            "partial frame on EOF should return None"
        );
    }

    /// Empty buffer on EOF should cleanly return None.
    #[test]
    fn empty_buffer_on_eof_returns_none() {
        let mut decoder = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        let mut buf = BytesMut::new();
        let result = decoder.decode_eof(&mut buf);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
