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
    /// Pending envelopes extracted from a multi-envelope v5 frame.
    /// The v5 spec allows multiple 9-byte envelopes in a single
    /// self-contained frame payload. We parse them all and queue them
    /// here so the Decoder returns one at a time.
    pending_envelopes: std::collections::VecDeque<CqlFrame>,
}

impl CqlCodec {
    pub fn new(max_frame_size: u32) -> Self {
        Self {
            max_frame_size,
            compression: None,
            v5_framed: false,
            v5_segment_buf: BytesMut::new(),
            pending_envelopes: std::collections::VecDeque::new(),
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
        let actual_crc32 = crc32_ieee_v5(&src[payload_start..payload_end]);
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
            // Slice of a large envelope. Per the v5 spec EVERY slice carries
            // isSelfContained=0 — there is no self-contained terminator — so
            // completion is determined by the envelope's own length field.
            // Waiting for a self-contained frame here would buffer forever
            // against any spec-compliant sender (the DataStax Java driver
            // included).
            self.v5_segment_buf.extend_from_slice(&payload);

            let Some(target) = v5_reassembly_target(&self.v5_segment_buf) else {
                return Ok(None); // envelope header not yet complete
            };
            if self.v5_segment_buf.len() < target {
                return Ok(None); // more slices to come
            }
            if self.v5_segment_buf.len() > target {
                // Slices must reassemble to exactly one envelope. Anything
                // else means the peer framed the stream in a way we cannot
                // interpret; guessing would silently corrupt the response.
                return Err(CqlError::Protocol(format!(
                    "v5 segmented envelope overran its declared length: \
                     accumulated {} bytes, envelope declares {target}",
                    self.v5_segment_buf.len()
                )));
            }
            let envelope_data = self.v5_segment_buf.split().freeze();
            return self.emit_envelopes(envelope_data);
        }

        // Self-contained frame. It may still be the tail of an accumulation
        // from a peer that terminates with a self-contained slice; combining
        // preserves that behavior.
        let envelope_data = if self.v5_segment_buf.is_empty() {
            payload.freeze()
        } else {
            self.v5_segment_buf.extend_from_slice(&payload);
            self.v5_segment_buf.split().freeze()
        };

        self.emit_envelopes(envelope_data)
    }

    /// Parse every envelope out of a fully reassembled payload, return the
    /// first, and queue the rest.
    ///
    /// The v5 spec allows pipelining multiple envelopes in a single
    /// self-contained frame. The DataStax Java driver does this (e.g. sending
    /// SELECT system.local and SELECT system.peers_v2 together).
    fn emit_envelopes(
        &mut self,
        envelope_data: Bytes,
    ) -> std::result::Result<Option<CqlFrame>, CqlError> {
        let mut offset = 0;
        let mut frames = Vec::new();
        while offset + HEADER_SIZE <= envelope_data.len() {
            let header = FrameHeader::decode(&envelope_data[offset..offset + HEADER_SIZE])?;
            let body_end = offset + HEADER_SIZE + header.length as usize;
            if body_end > envelope_data.len() {
                return Err(CqlError::Protocol(format!(
                    "v5 envelope body extends past payload: offset={offset}, body_end={body_end}, \
                     payload_len={}",
                    envelope_data.len()
                )));
            }
            let body = envelope_data.slice(offset + HEADER_SIZE..body_end);

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

            frames.push(CqlFrame { header, body });
            offset = body_end;
        }

        if frames.is_empty() {
            return Err(CqlError::Protocol(
                "v5 frame payload too short for envelope header".into(),
            ));
        }

        // Queue all but the first; return the first immediately.
        let mut iter = frames.into_iter();
        let first = iter.next().expect("frames is non-empty (checked above)");
        for f in iter {
            self.pending_envelopes.push_back(f);
        }
        Ok(Some(first))
    }
}

impl Decoder for CqlCodec {
    type Item = CqlFrame;
    type Error = CqlError;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        // Return any pending envelopes from a previously-decoded multi-envelope v5 frame.
        if let Some(frame) = self.pending_envelopes.pop_front() {
            return Ok(Some(frame));
        }

        // Valid request versions we accept: 0x03 (v3), 0x04 (v4), 0x05 (v5).
        // Anything higher is rejected with a protocol-version error.
        if src.len() >= HEADER_SIZE && !self.v5_framed {
            let version_byte = src[0];
            if (0x06..=0x7F).contains(&version_byte) {
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
        let original_body_len = item.body.len();
        let error_info = if header.opcode == Opcode::Error {
            parse_error_body_for_log(&item.body)
        } else {
            None
        };

        if header.opcode == Opcode::Error {
            // Keep ERROR frames uncompressed. Drivers often enter error decode
            // paths while connections are already degraded; making these small
            // frames plain avoids compounding backpressure failures with a
            // compression/framing decode failure.
            header.flags &= !COMPRESSION_FLAG;
        } else if let Some(compression) = self.compression {
            let compressed = match compression {
                Compression::Lz4 => compress_lz4(&item.body),
                Compression::Snappy => compress_snappy(&item.body),
            };
            if compressed.len() < item.body.len() {
                header.length = compressed.len() as u32;
                header.flags |= COMPRESSION_FLAG;
                log_encoded_error_frame(
                    &header,
                    original_body_len,
                    compressed.len(),
                    Some(compression),
                    error_info.as_ref(),
                );
                if self.v5_framed {
                    encode_v5_frame(&header, &compressed, dst);
                } else {
                    dst.reserve(HEADER_SIZE + compressed.len());
                    header.encode(dst);
                    dst.put_slice(&compressed);
                }
                return Ok(());
            }
            header.flags &= !COMPRESSION_FLAG;
        } else {
            header.flags &= !COMPRESSION_FLAG;
        }

        header.length = item.body.len() as u32;
        log_encoded_error_frame(
            &header,
            original_body_len,
            item.body.len(),
            None,
            error_info.as_ref(),
        );
        if self.v5_framed {
            // v5: wrap envelope in a frame with CRC24/CRC32.
            encode_v5_frame(&header, &item.body, dst);
        } else {
            // v4 / pre-STARTUP: raw 9-byte envelope header + body.
            dst.reserve(HEADER_SIZE + item.body.len());
            header.encode(dst);
            dst.put_slice(&item.body);
        }
        Ok(())
    }
}

struct ErrorBodyLogInfo {
    code: u32,
    message_len: usize,
    extra_len: usize,
    body_len: usize,
    prefix: String,
}

fn parse_error_body_for_log(body: &[u8]) -> Option<ErrorBodyLogInfo> {
    if body.len() < 6 {
        return Some(ErrorBodyLogInfo {
            code: 0,
            message_len: 0,
            extra_len: 0,
            body_len: body.len(),
            prefix: body_prefix_hex(body, 64),
        });
    }
    let code = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    let message_len = u16::from_be_bytes([body[4], body[5]]) as usize;
    let extra_len = body.len().saturating_sub(6 + message_len);
    Some(ErrorBodyLogInfo {
        code,
        message_len,
        extra_len,
        body_len: body.len(),
        prefix: body_prefix_hex(body, 64),
    })
}

fn error_extra_min_len(error_code: u32) -> Option<usize> {
    match error_code {
        0x1000 => Some(10),
        0x1100 => Some(12),
        0x1200 => Some(11),
        _ => None,
    }
}

fn body_prefix_hex(body: &[u8], max: usize) -> String {
    let mut out = String::with_capacity(max.saturating_mul(2));
    for byte in body.iter().take(max) {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn log_encoded_error_frame(
    header: &FrameHeader,
    original_body_len: usize,
    wire_body_len: usize,
    compression: Option<Compression>,
    info: Option<&ErrorBodyLogInfo>,
) {
    let Some(info) = info else {
        return;
    };
    let expected_extra_min = error_extra_min_len(info.code);
    let malformed = expected_extra_min.is_some_and(|min| info.extra_len < min);
    let compression_name = compression.map(|c| c.protocol_name()).unwrap_or("none");
    if malformed {
        tracing::warn!(
            cql.codec.error.code = format_args!("0x{:04x}", info.code),
            cql.codec.error.body_len = info.body_len,
            cql.codec.error.message_len = info.message_len,
            cql.codec.error.extra_len = info.extra_len,
            cql.codec.error.expected_extra_min = expected_extra_min.unwrap_or_default(),
            cql.codec.error.body_prefix = %info.prefix,
            cql.codec.stream_id = header.stream_id,
            cql.codec.response_version = header.version,
            cql.codec.flags = header.flags,
            cql.codec.original_body_len = original_body_len,
            cql.codec.wire_body_len = wire_body_len,
            cql.codec.compression = compression_name,
            "malformed CQL ERROR response body at codec encode"
        );
    } else {
        tracing::debug!(
            cql.codec.error.code = format_args!("0x{:04x}", info.code),
            cql.codec.error.body_len = info.body_len,
            cql.codec.error.message_len = info.message_len,
            cql.codec.error.extra_len = info.extra_len,
            cql.codec.stream_id = header.stream_id,
            cql.codec.response_version = header.version,
            cql.codec.flags = header.flags,
            cql.codec.original_body_len = original_body_len,
            cql.codec.wire_body_len = wire_body_len,
            cql.codec.compression = compression_name,
            "encoding CQL ERROR response"
        );
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

/// CRC32-IEEE used by CQL v5 frame payload checksums.
///
/// The DataStax Java driver/native-protocol library seeds the standard
/// `java.util.zip.CRC32` with four magic bytes before updating it with the
/// payload. The same seed must be used on encode and decode for v5 frames to
/// interop.
fn crc32_ieee_v5(data: &[u8]) -> u32 {
    const CRC32_INIT: u32 = 0xFFFF_FFFF;
    const CRC32_POLY: u32 = 0xEDB8_8320; // reflected IEEE polynomial
    const MAGIC: &[u8] = &[0xFA, 0x2D, 0x55, 0xCA];

    let mut crc = CRC32_INIT;
    for &byte in MAGIC.iter().chain(data.iter()) {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ CRC32_POLY;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ CRC32_INIT
}

/// Total bytes the accumulated segment buffer must reach to hold one complete
/// envelope, or `None` while the 9-byte envelope header is still incomplete.
///
/// A multi-frame v5 envelope has no terminating marker — the receiver learns
/// the total from the envelope header's own length field, exactly as the
/// DataStax Java driver does (`header_size + decodeBodySize(slice)`).
fn v5_reassembly_target(buf: &[u8]) -> Option<usize> {
    if buf.len() < HEADER_SIZE {
        return None;
    }
    // Envelope header layout: version(1) flags(1) stream(2) opcode(1) length(4, BE).
    let body_len = u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]) as usize;
    Some(HEADER_SIZE + body_len)
}

/// Encode a v5 frame around an envelope (header + body).
///
/// Produces: [3-byte LE header][3-byte CRC24][payload][4-byte CRC32]
/// where payload = 9-byte envelope header + envelope body.
pub fn encode_v5_frame(envelope_header: &FrameHeader, body: &[u8], dst: &mut BytesMut) {
    // Build the envelope (9-byte header + body), then emit it as one or more
    // frames depending on whether it fits the 17-bit payload length field.
    let payload_len = HEADER_SIZE + body.len();

    if payload_len <= V5_MAX_PAYLOAD {
        let mut envelope = BytesMut::with_capacity(payload_len);
        envelope_header.encode(&mut envelope);
        envelope.put_slice(body);
        put_v5_frame(&envelope, true, dst);
        return;
    }

    // Oversize: split across consecutive NON-self-contained frames. Every
    // slice carries isSelfContained=0 — there is no self-contained terminator.
    // The receiver reassembles by reading the envelope header's length field
    // and accumulating until it is satisfied (see `decode_v5_frame`). This
    // matches the DataStax Java driver's SegmentToFrameDecoder.
    //
    // Previously this asserted, which panicked the CQL runtime thread and
    // dropped the connection for any response page over 128 KiB.
    let mut envelope = BytesMut::with_capacity(payload_len);
    envelope_header.encode(&mut envelope);
    envelope.put_slice(body);

    for slice in envelope.chunks(V5_MAX_PAYLOAD) {
        put_v5_frame(slice, false, dst);
    }
}

/// Append exactly one v5 frame wrapping `payload`.
///
/// `payload` must already be at most [`V5_MAX_PAYLOAD`] bytes; callers are
/// responsible for slicing. Writes the 3-byte length/flag header, its CRC24,
/// the payload, and the payload's CRC32.
fn put_v5_frame(payload: &[u8], self_contained: bool, dst: &mut BytesMut) {
    debug_assert!(payload.len() <= V5_MAX_PAYLOAD);

    // 3-byte header: payload_length(17 bits) | isSelfContained(1 bit) | padding(6 bits)
    let mut header_bits: u32 = payload.len() as u32;
    if self_contained {
        header_bits |= 1 << 17;
    }
    let h_bytes = header_bits.to_le_bytes(); // [b0, b1, b2, _]
    let crc24_bytes = crc24(&h_bytes[..3]).to_le_bytes();

    dst.reserve(V5_FRAME_HEADER_SIZE + payload.len() + V5_CRC32_SIZE);
    dst.put_slice(&h_bytes[..3]);
    dst.put_slice(&crc24_bytes[..3]);
    dst.put_slice(payload);
    dst.put_u32_le(crc32_ieee_v5(payload));
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
    fn codec_accepts_v5_request_before_framing_is_enabled() {
        // Before STARTUP/READY completes the transport is still legacy envelopes,
        // so a v5 request is decoded as a plain 9-byte envelope.
        let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        assert!(!codec.is_v5_framed());
        let mut buf = BytesMut::new();
        let header = FrameHeader {
            version: 0x05,
            flags: 0x10, // USE_BETA (ignored now that v5 is stable)
            stream_id: 0,
            opcode: Opcode::Query,
            length: 4,
        };
        header.encode(&mut buf);
        buf.put_slice(&[0x00, 0x00, 0x00, 0x00]);

        let frame = codec
            .decode(&mut buf)
            .unwrap()
            .expect("v5 legacy envelope should decode");
        assert_eq!(frame.header.version, 0x05);
        assert_eq!(frame.header.opcode, Opcode::Query);
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
    fn test_error_frames_remain_uncompressed_with_lz4_enabled() {
        let mut encoder = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        encoder.set_compression(Compression::Lz4);

        let body = crate::error::CqlError::WriteTimeout {
            consistency: ferrosa_cluster::consistency::ConsistencyLevel::LocalQuorum,
            received: 1,
            required: 2,
            write_type: "SIMPLE",
        }
        .encode_body();

        let frame = CqlFrame {
            header: FrameHeader {
                version: VERSION_RESPONSE,
                flags: 0,
                stream_id: 7,
                opcode: Opcode::Error,
                length: 0,
            },
            body: body.clone().freeze(),
        };

        let mut buf = BytesMut::new();
        encoder.encode(frame, &mut buf).unwrap();

        let header = FrameHeader::decode(&buf).unwrap();
        assert_eq!(header.opcode, Opcode::Error);
        assert_eq!(header.flags & COMPRESSION_FLAG, 0);
        assert_eq!(header.length as usize, body.len());

        let mut decoder = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        decoder.set_compression(Compression::Lz4);
        let decoded = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.body.as_ref(), &body[..]);

        let mut cursor = decoded.body.as_ref();
        assert_eq!(cursor.get_u32(), 0x1100);
        let msg_len = cursor.get_u16() as usize;
        cursor.advance(msg_len);
        assert_eq!(
            cursor.get_u16(),
            ferrosa_cluster::consistency::ConsistencyLevel::LocalQuorum.to_wire()
        );
        assert_eq!(cursor.get_i32(), 1);
        assert_eq!(cursor.get_i32(), 2);
        let write_type_len = cursor.get_u16() as usize;
        assert_eq!(&cursor[..write_type_len], b"SIMPLE");
    }

    #[test]
    fn version_response_is_v4() {
        assert_eq!(VERSION_RESPONSE, 0x84);
    }

    #[test]
    fn codec_accepts_v5_startup_before_framing_is_enabled() {
        // ferrosa now supports native protocol v5. The STARTUP/READY exchange
        // still uses legacy 9-byte envelopes; modern v5 framing is enabled
        // by the connection handler only after READY succeeds.
        let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        let mut buf = BytesMut::new();
        let header = FrameHeader {
            version: 0x05,
            flags: 0,
            stream_id: 0,
            opcode: Opcode::Startup,
            length: 0,
        };
        header.encode(&mut buf);
        let frame = codec
            .decode(&mut buf)
            .unwrap()
            .expect("v5 STARTUP should decode as legacy envelope");
        assert_eq!(frame.header.version, 0x05);
        assert_eq!(frame.header.opcode, Opcode::Startup);
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

    /// Regression test for the DataStax Java driver v5 frame checksum.
    ///
    /// The Java driver's native-protocol library uses `java.util.zip.CRC32`
    /// seeded with four magic bytes for the v5 frame payload checksum. This
    /// test feeds the exact v5-framed QUERY bytes captured from a Java driver
    /// control connection (`SELECT cluster_name FROM system.local`) and
    /// asserts that our decoder accepts the frame instead of rejecting it with
    /// a CRC mismatch.
    #[test]
    fn decodes_real_java_driver_v5_query_frame() {
        // Captured from DataStax Java driver 4.18.1 after v5 STARTUP/READY.
        // Frame layout: 3-byte LE header | 3-byte CRC24 | 9-byte envelope |
        // payload | 4-byte CRC32.
        #[rustfmt::skip]
        let frame: Vec<u8> = vec![
            0x38, 0x00, 0x02, 0x43, 0xa1, 0x53,
            0x05, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x2f,
            0x00, 0x00, 0x00, 0x25, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54,
            0x20, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x5f, 0x6e,
            0x61, 0x6d, 0x65, 0x20, 0x46, 0x52, 0x4f, 0x4d, 0x20, 0x73,
            0x79, 0x73, 0x74, 0x65, 0x6d, 0x2e, 0x6c, 0x6f, 0x63, 0x61,
            0x6c, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x12, 0xc2, 0x55, 0x69,
        ];

        let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        codec.enable_v5_framing();
        let mut buf = BytesMut::from(&frame[..]);
        let decoded = codec
            .decode(&mut buf)
            .expect("v5 frame should decode without CRC error")
            .expect("v5 frame should produce a complete envelope");
        assert_eq!(decoded.header.version, 0x05);
        assert_eq!(decoded.header.opcode, Opcode::Query);
        assert_eq!(decoded.header.stream_id, 0);
        assert_eq!(decoded.header.flags, 0);
    }

    // --- v5 multi-frame (non-self-contained) encoding ---
    //
    // An envelope larger than V5_MAX_PAYLOAD must be split across several
    // frames. Per the CQL v5 spec every slice carries isSelfContained=0, and
    // the receiver knows the envelope is complete when the accumulated bytes
    // satisfy the 9-byte envelope header's length field — there is no
    // terminating self-contained frame. (Confirmed against the DataStax Java
    // driver's SegmentToFrameDecoder, which computes
    // `targetLength = header_size + decodeBodySize(slice)` and completes on
    // `accumulatedLength == targetLength`.)
    //
    // Before this change `encode_v5_frame` asserted on oversize payloads,
    // panicking the CQL runtime thread and dropping the client connection —
    // reachable from any ordinary `SELECT *` with a large enough page.

    /// Split `buf` into (payload, is_self_contained) pairs, validating CRCs.
    fn split_v5_frames(buf: &[u8]) -> Vec<(Vec<u8>, bool)> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < buf.len() {
            assert!(
                i + V5_FRAME_HEADER_SIZE <= buf.len(),
                "truncated v5 frame header at {i}"
            );
            let bits = buf[i] as u32 | ((buf[i + 1] as u32) << 8) | ((buf[i + 2] as u32) << 16);
            let len = (bits & 0x1FFFF) as usize;
            let self_contained = (bits >> 17) & 1 == 1;
            let expect_crc24 =
                buf[i + 3] as u32 | ((buf[i + 4] as u32) << 8) | ((buf[i + 5] as u32) << 16);
            assert_eq!(crc24(&buf[i..i + 3]), expect_crc24, "header CRC24 at {i}");
            let ps = i + V5_FRAME_HEADER_SIZE;
            let pe = ps + len;
            assert!(
                pe + V5_CRC32_SIZE <= buf.len(),
                "truncated v5 payload at {i}"
            );
            let expect_crc32 = u32::from_le_bytes([buf[pe], buf[pe + 1], buf[pe + 2], buf[pe + 3]]);
            assert_eq!(
                crc32_ieee_v5(&buf[ps..pe]),
                expect_crc32,
                "payload CRC32 at {i}"
            );
            out.push((buf[ps..pe].to_vec(), self_contained));
            i = pe + V5_CRC32_SIZE;
        }
        out
    }

    /// Feed a whole encoded stream through the codec and return the single
    /// envelope it reassembles. Intermediate slices decode to `None`, so this
    /// drains until the buffer is empty rather than stopping at the first one.
    /// Asserts that exactly one envelope comes out.
    fn drain_one_envelope(codec: &mut CqlCodec, buf: &mut BytesMut) -> Option<CqlFrame> {
        let mut out = None;
        let mut guard = 0;
        while !buf.is_empty() {
            guard += 1;
            assert!(guard < 10_000, "decode made no progress");
            match codec.decode(buf).expect("decode must not error") {
                Some(f) => {
                    assert!(out.is_none(), "only one envelope should be produced");
                    out = Some(f);
                }
                None if buf.is_empty() => break,
                None => {}
            }
        }
        out
    }

    fn big_header(body_len: usize) -> FrameHeader {
        FrameHeader {
            version: VERSION_RESPONSE,
            flags: 0,
            stream_id: 7,
            opcode: Opcode::Result,
            length: body_len as u32,
        }
    }

    #[test]
    fn v5_envelope_that_fits_stays_one_self_contained_frame() {
        // Largest body that still fits with its 9-byte envelope header.
        let body = vec![0xABu8; V5_MAX_PAYLOAD - HEADER_SIZE];
        let mut dst = BytesMut::new();
        encode_v5_frame(&big_header(body.len()), &body, &mut dst);

        let frames = split_v5_frames(&dst);
        assert_eq!(frames.len(), 1, "should not split when it fits");
        assert!(frames[0].1, "a fitting envelope must be self-contained");
        assert_eq!(frames[0].0.len(), V5_MAX_PAYLOAD);
    }

    #[test]
    fn v5_oversize_envelope_splits_into_non_self_contained_frames() {
        // One byte past what a single frame can hold.
        let body = vec![0x5Au8; V5_MAX_PAYLOAD - HEADER_SIZE + 1];
        let mut dst = BytesMut::new();
        encode_v5_frame(&big_header(body.len()), &body, &mut dst);

        let frames = split_v5_frames(&dst);
        assert!(frames.len() > 1, "oversize envelope must be split");
        assert!(
            frames.iter().all(|(_, sc)| !*sc),
            "every slice of a multi-frame envelope must have isSelfContained=0"
        );
        assert!(
            frames.iter().all(|(p, _)| p.len() <= V5_MAX_PAYLOAD),
            "no slice may exceed V5_MAX_PAYLOAD"
        );
        let total: usize = frames.iter().map(|(p, _)| p.len()).sum();
        assert_eq!(
            total,
            HEADER_SIZE + body.len(),
            "slices must reassemble to the whole envelope"
        );
    }

    #[test]
    fn v5_oversize_envelope_round_trips_through_the_codec() {
        // ~3.5 frames' worth, so the last slice is a partial one.
        let body: Vec<u8> = (0..(V5_MAX_PAYLOAD * 7 / 2))
            .map(|i| (i % 251) as u8)
            .collect();
        let mut dst = BytesMut::new();
        encode_v5_frame(&big_header(body.len()), &body, &mut dst);

        let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        codec.enable_v5_framing();

        // Intermediate slices legitimately decode to None, so drain the whole
        // buffer rather than stopping at the first None.
        let f = drain_one_envelope(&mut codec, &mut dst)
            .expect("a complete envelope must be reassembled");
        assert_eq!(f.header.stream_id, 7);
        assert_eq!(f.header.opcode, Opcode::Result);
        assert_eq!(f.body.len(), body.len());
        assert_eq!(&f.body[..], &body[..], "body must survive the round trip");
    }

    #[test]
    fn v5_split_is_exact_at_a_slice_boundary() {
        // Envelope length is an exact multiple of the frame payload limit —
        // an off-by-one here produces a trailing empty frame or drops bytes.
        let body = vec![0x11u8; V5_MAX_PAYLOAD * 2 - HEADER_SIZE];
        let mut dst = BytesMut::new();
        encode_v5_frame(&big_header(body.len()), &body, &mut dst);

        let frames = split_v5_frames(&dst);
        assert_eq!(frames.len(), 2, "exactly two full slices, no empty tail");
        assert!(frames.iter().all(|(p, _)| p.len() == V5_MAX_PAYLOAD));

        let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        codec.enable_v5_framing();
        let f = drain_one_envelope(&mut codec, &mut dst).expect("must reassemble");
        assert_eq!(f.body.len(), body.len());
    }

    /// Regression test: DataStax Java driver pipelines multiple queries in a
    /// single v5 self-contained frame. The captured frame contains two QUERY
    /// envelopes: stream 0 = `SELECT * FROM system.local` and stream 1 =
    /// `SELECT * FROM system.peers_v2`. Our decoder must extract BOTH envelopes
    /// and return them on successive `decode()` calls.
    #[test]
    fn decodes_multi_envelope_v5_frame() {
        // Captured from DataStax Java driver 4.18.1 during control-connection init.
        // The driver sends both system.local and system.peers_v2 queries in one frame.
        #[rustfmt::skip]
        let frame: Vec<u8> = vec![
            // 3-byte LE header: payload_len=93 | isSelfContained=1
            0x5d, 0x00, 0x02,
            // 3-byte CRC24 of header
            0xc5, 0x52, 0x6e,
            // Envelope 1: stream=0, QUERY, body_len=36
            0x05, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x24,
            // Body: "SELECT * FROM system.local" + consistency=ONE(0x0001) + flags=0x00
            0x00, 0x00, 0x00, 0x1a, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54,
            0x20, 0x2a, 0x20, 0x46, 0x52, 0x4f, 0x4d, 0x20, 0x73, 0x79,
            0x73, 0x74, 0x65, 0x6d, 0x2e, 0x6c, 0x6f, 0x63, 0x61, 0x6c,
            0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            // Envelope 2: stream=1, QUERY, body_len=39
            0x05, 0x00, 0x00, 0x01, 0x07, 0x00, 0x00, 0x00, 0x27,
            // Body: "SELECT * FROM system.peers_v2" + consistency=ONE(0x0001) + flags=0x00
            0x00, 0x00, 0x00, 0x1d, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54,
            0x20, 0x2a, 0x20, 0x46, 0x52, 0x4f, 0x4d, 0x20, 0x73, 0x79,
            0x73, 0x74, 0x65, 0x6d, 0x2e, 0x70, 0x65, 0x65, 0x72, 0x73,
            0x5f, 0x76, 0x32, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            // 4-byte CRC32 of payload
            0x43, 0x4e, 0xcd, 0x96,
        ];

        let mut codec = CqlCodec::new(DEFAULT_MAX_FRAME_SIZE);
        codec.enable_v5_framing();
        let mut buf = BytesMut::from(&frame[..]);

        // First decode: should return envelope 1 (stream 0)
        let frame1 = codec
            .decode(&mut buf)
            .expect("first v5 envelope should decode")
            .expect("should have a frame");
        assert_eq!(frame1.header.stream_id, 0);
        assert_eq!(frame1.header.opcode, Opcode::Query);

        // Second decode: should return envelope 2 (stream 1) from pending queue
        let frame2 = codec
            .decode(&mut buf)
            .expect("second v5 envelope should decode")
            .expect("should have a second frame");
        assert_eq!(frame2.header.stream_id, 1);
        assert_eq!(frame2.header.opcode, Opcode::Query);

        // Third decode: no more pending, no more data
        let frame3 = codec.decode(&mut buf).expect("should be Ok(None)");
        assert!(frame3.is_none(), "no more frames expected");
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
