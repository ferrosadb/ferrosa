use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::{NetError, Result};

/// Header size in bytes: version(1) + flags(1) + lane(1) + msg_type(1)
/// + stream_id(4) + length(4) = 12.
pub const HEADER_SIZE: usize = 12;

/// Flag bits.
pub const FLAG_COMPRESSED: u8 = 0x01;
pub const FLAG_STREAM_START: u8 = 0x02;
pub const FLAG_STREAM_END: u8 = 0x04;
pub const FLAG_FIRE_AND_FORGET: u8 = 0x08;

/// Priority lane for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Lane {
    Raft = 0,
    Data = 1,
    Bulk = 2,
}

impl Lane {
    pub fn timeout(&self) -> std::time::Duration {
        match self {
            Self::Raft => std::time::Duration::from_secs(1),
            Self::Data => std::time::Duration::from_secs(10),
            Self::Bulk => std::time::Duration::from_secs(60),
        }
    }
}

impl TryFrom<u8> for Lane {
    type Error = NetError;
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Raft),
            1 => Ok(Self::Data),
            2 => Ok(Self::Bulk),
            _ => Err(NetError::InvalidLane(value)),
        }
    }
}

/// Message type discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MsgType {
    // Lifecycle
    Handshake = 0x01,
    HandshakeAck = 0x02,
    Ping = 0x03,
    Pong = 0x04,
    ClusterInvite = 0x05,
    ClusterInviteAck = 0x06,
    // Raft
    RaftAppendEntries = 0x10,
    RaftAppendResponse = 0x11,
    RaftVote = 0x12,
    RaftVoteResponse = 0x13,
    RaftInstallSnapshot = 0x14,
    // Data
    MutationForward = 0x20,
    MutationAck = 0x21,
    ReadRequest = 0x22,
    ReadResponse = 0x23,
    RepairWrite = 0x24,
    RangeReadRequest = 0x25,
    RangeReadResponse = 0x26,
    // Streaming
    StreamStart = 0x30,
    StreamChunk = 0x31,
    StreamEnd = 0x32,
    // Pair
    PairWriteForward = 0x40,
    PairWriteAck = 0x41,
    PairCatchUp = 0x42,
    PairCatchUpResponse = 0x43,
    RoleSwap = 0x44,
    PairSchemaSync = 0x45,
    PairDdlForward = 0x46,
    PairDdlAck = 0x47,
    /// Atomic batch forwarded between pair nodes (batch_id prefix + all mutations).
    PairBatchForward = 0x48,
    /// Acknowledgment for a PairBatchForward.
    PairBatchAck = 0x49,
    // Batchlog
    BatchlogWrite = 0x50,
    BatchlogDelete = 0x51,
    BatchlogReplay = 0x52,
    // Index build coordination
    IndexBuildRequest = 0x60,
    IndexBuildComplete = 0x61,
    // Accord consensus
    AccordPreAccept = 0x70,
    AccordPreAcceptOK = 0x71,
    AccordAccept = 0x72,
    AccordAcceptOK = 0x73,
    AccordCommit = 0x74,
    AccordRead = 0x75,
    AccordReadOK = 0x76,
    AccordApply = 0x77,
    AccordApplyOK = 0x78,
    AccordRecover = 0x79,
    AccordRecoverOK = 0x7A,
    // Bootstrap coordination
    BootstrapComplete = 0x80,
    BootstrapCompleteAck = 0x81,
}

impl TryFrom<u8> for MsgType {
    type Error = NetError;
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0x01 => Ok(Self::Handshake),
            0x02 => Ok(Self::HandshakeAck),
            0x03 => Ok(Self::Ping),
            0x04 => Ok(Self::Pong),
            0x05 => Ok(Self::ClusterInvite),
            0x06 => Ok(Self::ClusterInviteAck),
            0x10 => Ok(Self::RaftAppendEntries),
            0x11 => Ok(Self::RaftAppendResponse),
            0x12 => Ok(Self::RaftVote),
            0x13 => Ok(Self::RaftVoteResponse),
            0x14 => Ok(Self::RaftInstallSnapshot),
            0x20 => Ok(Self::MutationForward),
            0x21 => Ok(Self::MutationAck),
            0x22 => Ok(Self::ReadRequest),
            0x23 => Ok(Self::ReadResponse),
            0x24 => Ok(Self::RepairWrite),
            0x25 => Ok(Self::RangeReadRequest),
            0x26 => Ok(Self::RangeReadResponse),
            0x30 => Ok(Self::StreamStart),
            0x31 => Ok(Self::StreamChunk),
            0x32 => Ok(Self::StreamEnd),
            0x40 => Ok(Self::PairWriteForward),
            0x41 => Ok(Self::PairWriteAck),
            0x42 => Ok(Self::PairCatchUp),
            0x43 => Ok(Self::PairCatchUpResponse),
            0x44 => Ok(Self::RoleSwap),
            0x45 => Ok(Self::PairSchemaSync),
            0x46 => Ok(Self::PairDdlForward),
            0x47 => Ok(Self::PairDdlAck),
            0x48 => Ok(Self::PairBatchForward),
            0x49 => Ok(Self::PairBatchAck),
            0x50 => Ok(Self::BatchlogWrite),
            0x51 => Ok(Self::BatchlogDelete),
            0x52 => Ok(Self::BatchlogReplay),
            0x60 => Ok(Self::IndexBuildRequest),
            0x61 => Ok(Self::IndexBuildComplete),
            0x70 => Ok(Self::AccordPreAccept),
            0x71 => Ok(Self::AccordPreAcceptOK),
            0x72 => Ok(Self::AccordAccept),
            0x73 => Ok(Self::AccordAcceptOK),
            0x74 => Ok(Self::AccordCommit),
            0x75 => Ok(Self::AccordRead),
            0x76 => Ok(Self::AccordReadOK),
            0x77 => Ok(Self::AccordApply),
            0x78 => Ok(Self::AccordApplyOK),
            0x79 => Ok(Self::AccordRecover),
            0x7A => Ok(Self::AccordRecoverOK),
            0x80 => Ok(Self::BootstrapComplete),
            0x81 => Ok(Self::BootstrapCompleteAck),
            _ => Err(NetError::UnknownMessageType(value)),
        }
    }
}

/// 12-byte wire frame header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    pub version: u8,
    pub flags: u8,
    pub lane: Lane,
    pub msg_type: MsgType,
    pub stream_id: u32,
    pub length: u32,
}

impl FrameHeader {
    /// Create a new frame header with version=1 and flags=0.
    pub fn new(msg_type: MsgType, lane: Lane, stream_id: u32, length: u32) -> Self {
        Self {
            version: 1,
            flags: 0,
            lane,
            msg_type,
            stream_id,
            length,
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u8(self.flags);
        buf.put_u8(self.lane as u8);
        buf.put_u8(self.msg_type as u8);
        buf.put_u32(self.stream_id);
        buf.put_u32(self.length);
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < HEADER_SIZE {
            return Err(NetError::Protocol("header too short".into()));
        }
        let version = buf[0];
        if version != 1 {
            return Err(NetError::Protocol(format!(
                "unsupported internode protocol version: {version}"
            )));
        }
        Ok(Self {
            version,
            flags: buf[1],
            lane: Lane::try_from(buf[2])?,
            msg_type: MsgType::try_from(buf[3])?,
            stream_id: u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]),
            length: u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]),
        })
    }
}

/// A raw internode frame: header + body bytes.
#[derive(Debug, Clone)]
pub struct Frame {
    pub header: FrameHeader,
    pub body: bytes::Bytes,
}

/// Codec for encoding/decoding internode frames on a TCP stream.
pub struct InternodeCodec {
    max_frame_body_size: u32,
}

impl InternodeCodec {
    pub fn new(max_frame_body_size: u32) -> Self {
        Self {
            max_frame_body_size,
        }
    }
}

impl Decoder for InternodeCodec {
    type Item = Frame;
    type Error = NetError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }
        let header = FrameHeader::decode(&src[..HEADER_SIZE])?;
        if header.length > self.max_frame_body_size {
            return Err(NetError::FrameTooLarge {
                size: header.length,
                max: self.max_frame_body_size,
            });
        }
        let total = HEADER_SIZE + header.length as usize;
        if src.len() < total {
            src.reserve(total - src.len());
            return Ok(None);
        }
        src.advance(HEADER_SIZE);
        let body = src.split_to(header.length as usize).freeze();
        Ok(Some(Frame { header, body }))
    }
}

impl Encoder<Frame> for InternodeCodec {
    type Error = NetError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<()> {
        let mut header = item.header;
        let body_len = u32::try_from(item.body.len())
            .map_err(|_| NetError::Protocol("frame body exceeds u32::MAX".into()))?;
        header.length = body_len;
        dst.reserve(HEADER_SIZE + item.body.len());
        header.encode(dst);
        dst.put_slice(&item.body);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lane_from_u8_valid() {
        assert_eq!(Lane::try_from(0).unwrap(), Lane::Raft);
        assert_eq!(Lane::try_from(1).unwrap(), Lane::Data);
        assert_eq!(Lane::try_from(2).unwrap(), Lane::Bulk);
    }

    #[test]
    fn lane_from_u8_invalid() {
        assert!(Lane::try_from(3).is_err());
        assert!(Lane::try_from(255).is_err());
    }

    #[test]
    fn frame_header_encode_decode_roundtrip() {
        let header = FrameHeader {
            version: 1,
            flags: 0,
            lane: Lane::Data,
            msg_type: MsgType::Ping,
            stream_id: 42,
            length: 1024,
        };
        let mut buf = BytesMut::with_capacity(HEADER_SIZE);
        header.encode(&mut buf);
        assert_eq!(buf.len(), HEADER_SIZE);

        let decoded = FrameHeader::decode(&buf).unwrap();
        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.lane, Lane::Data);
        assert_eq!(decoded.msg_type, MsgType::Ping);
        assert_eq!(decoded.stream_id, 42);
        assert_eq!(decoded.length, 1024);
    }

    #[test]
    fn codec_decode_complete_frame() {
        let mut codec = InternodeCodec::new(256 * 1024 * 1024);
        let mut buf = BytesMut::new();
        let header = FrameHeader {
            version: 1,
            flags: 0,
            lane: Lane::Raft,
            msg_type: MsgType::Ping,
            stream_id: 1,
            length: 4,
        };
        header.encode(&mut buf);
        buf.put_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.header.msg_type, MsgType::Ping);
        assert_eq!(frame.body.len(), 4);
    }

    #[test]
    fn codec_decode_incomplete_header() {
        let mut codec = InternodeCodec::new(256 * 1024 * 1024);
        let mut buf = BytesMut::from(&[0x01, 0x00, 0x00][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn codec_decode_incomplete_body() {
        let mut codec = InternodeCodec::new(256 * 1024 * 1024);
        let mut buf = BytesMut::new();
        let header = FrameHeader {
            version: 1,
            flags: 0,
            lane: Lane::Raft,
            msg_type: MsgType::Ping,
            stream_id: 0,
            length: 100,
        };
        header.encode(&mut buf);
        buf.put_slice(&[0u8; 50]); // only 50 of 100 bytes
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn codec_reject_oversized_frame() {
        let mut codec = InternodeCodec::new(1024);
        let mut buf = BytesMut::new();
        let header = FrameHeader {
            version: 1,
            flags: 0,
            lane: Lane::Data,
            msg_type: MsgType::Ping,
            stream_id: 0,
            length: 2048,
        };
        header.encode(&mut buf);
        let err = codec.decode(&mut buf).unwrap_err();
        assert!(matches!(err, NetError::FrameTooLarge { .. }));
    }

    #[test]
    fn msg_type_repair_write() {
        let mt = MsgType::try_from(0x24u8).unwrap();
        assert_eq!(mt, MsgType::RepairWrite);
        assert_eq!(mt as u8, 0x24);
    }

    #[test]
    fn index_build_request_msg_type_roundtrip() {
        let val = MsgType::IndexBuildRequest as u8;
        let parsed = MsgType::try_from(val).unwrap();
        assert_eq!(parsed, MsgType::IndexBuildRequest);
    }

    #[test]
    fn index_build_complete_msg_type_roundtrip() {
        let val = MsgType::IndexBuildComplete as u8;
        let parsed = MsgType::try_from(val).unwrap();
        assert_eq!(parsed, MsgType::IndexBuildComplete);
    }

    #[test]
    fn codec_encode_decode_roundtrip() {
        let mut codec = InternodeCodec::new(256 * 1024 * 1024);
        let frame = Frame {
            header: FrameHeader {
                version: 1,
                flags: 0,
                lane: Lane::Bulk,
                msg_type: MsgType::StreamChunk,
                stream_id: 99,
                length: 0, // set by encoder
            },
            body: bytes::Bytes::from_static(b"hello world"),
        };
        let mut buf = BytesMut::new();
        codec.encode(frame.clone(), &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.header.lane, Lane::Bulk);
        assert_eq!(decoded.header.msg_type, MsgType::StreamChunk);
        assert_eq!(decoded.header.stream_id, 99);
        assert_eq!(decoded.body, bytes::Bytes::from_static(b"hello world"));
    }
}
