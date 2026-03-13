//! CQL native protocol v5 frame encoding and decoding.
//!
//! Each frame has a 9-byte header (version, flags, stream ID, opcode,
//! length) followed by a body of `length` bytes.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::CqlError;

/// CQL native protocol v5 version byte for requests.
pub const VERSION_REQUEST: u8 = 0x05;
/// CQL native protocol v5 version byte for responses.
pub const VERSION_RESPONSE: u8 = 0x85;
/// Size of the frame header in bytes.
pub const HEADER_SIZE: usize = 9;
/// Default maximum frame body size: 256 MiB.
pub const DEFAULT_MAX_FRAME_SIZE: u32 = 256 * 1024 * 1024;

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
}

impl CqlCodec {
    pub fn new(max_frame_size: u32) -> Self {
        Self { max_frame_size }
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
        let body = src.split_to(header.length as usize).freeze();
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
        header.length = item.body.len() as u32;
        dst.reserve(HEADER_SIZE + item.body.len());
        header.encode(dst);
        dst.put(item.body);
        Ok(())
    }
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
}
