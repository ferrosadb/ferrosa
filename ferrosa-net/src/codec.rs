use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::{NetError, Result};

/// Header size in bytes: version(1) + flags(1) + lane(1) + msg_type(1)
/// + stream_id(4) + length(4) + trace_context(32) = 44.
///
/// The trace context is a fixed 32-byte field:
/// - bytes 0..16:  trace_id (128-bit)
/// - bytes 16..24: span_id  (64-bit)
/// - bytes 24..32: flags    (64-bit)
///
/// All-zero means no active trace.
pub const HEADER_SIZE: usize = 44;
pub const LEGACY_FRAME_VERSION: u8 = 1;
pub const CAPNP_FRAME_VERSION: u8 = 2;

/// Size of the trace context field in the frame header.
pub const TRACE_CONTEXT_SIZE: usize = 32;

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

    pub const fn index(self) -> usize {
        self as usize
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Raft => "raft",
            Self::Data => "data",
            Self::Bulk => "bulk",
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
    TruncateForward = 0x27,
    TruncateAck = 0x28,
    // Anti-entropy repair (cluster/repair) — three request/response pairs.
    // Initiator → peer: build Merkle for table range. Peer → initiator: tree.
    RepairMerkleRequest = 0x29,
    RepairMerkleResponse = 0x2A,
    // Initiator → peer: send partitions in a token sub-range.
    RepairFetchRequest = 0x2B,
    RepairFetchResponse = 0x2C,
    // Initiator → peer: apply these partitions (LWW). Peer → initiator: ack.
    RepairApplyRequest = 0x2D,
    RepairApplyResponse = 0x2E,
    // ADR-020 streaming range read — multi-message RPC keyed by
    // request_id, terminated by RangeReadStreamDone, cancellable via
    // RangeReadStreamCancel, kept alive by RangeReadStreamHeartbeat.
    RangeReadStreamRequest = 0x36,
    RangeReadStreamChunk = 0x37,
    RangeReadStreamHeartbeat = 0x38,
    RangeReadStreamDone = 0x39,
    RangeReadStreamCancel = 0x3A,
    // Streaming (row-based)
    StreamStart = 0x30,
    StreamChunk = 0x31,
    StreamEnd = 0x32,
    // Streaming (SSTable file-based)
    SstableStreamStart = 0x33,
    SstableStreamChunk = 0x34,
    SstableStreamEnd = 0x35,
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
    // Secondary index scatter-gather
    IndexReadRequest = 0x62,
    IndexReadResponse = 0x63,
    // Full-text index scatter-gather (fts_match across every node's local FTI)
    FulltextSearchRequest = 0x64,
    FulltextSearchResponse = 0x65,
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
    // Cluster-mode membership proposal forwarding (non-leader → leader).
    // Renamed from ClusterRaftForward — W1.13 carries a typed
    // `MembershipOp` payload so leaders can dispatch on the operation
    // without having to peek inside the bincoded RaftCommand.
    ClusterMembershipForward = 0x82,
    ClusterMembershipForwardAck = 0x83,
}

impl MsgType {
    /// Returns true for Raft consensus message types.
    pub fn is_raft(self) -> bool {
        matches!(
            self,
            Self::RaftAppendEntries
                | Self::RaftAppendResponse
                | Self::RaftVote
                | Self::RaftVoteResponse
                | Self::RaftInstallSnapshot
        )
    }

    /// Returns true for the streaming range-read RESPONSE frames that
    /// belong to a single ordered, per-`request_id` stream: chunk,
    /// heartbeat, and done.
    ///
    /// These frames MUST be dispatched in wire order. The coordinator's
    /// `StreamFrameRouter` enforces a strict, contiguous chunk `seq` and
    /// closes the route on the first gap; dispatching one task per frame
    /// lets chunk `seq=N+1` overtake `seq=N` under tokio scheduling and
    /// trips that check mid-stream, surfacing as
    /// `ChannelClosedBeforeDone`. The fault stays invisible until a
    /// single response carries more than one chunk — which is exactly
    /// what intra-partition row fragmentation (bounded full-scan memory)
    /// introduced for wide partitions.
    ///
    /// The producer-side `RangeReadStreamRequest` is deliberately
    /// excluded: it runs a long storage read and must stay off the
    /// frame-reader path. `RangeReadStreamCancel` is likewise excluded —
    /// it is not part of the ordered response stream.
    pub fn is_ordered_stream_response(self) -> bool {
        matches!(
            self,
            Self::RangeReadStreamChunk | Self::RangeReadStreamHeartbeat | Self::RangeReadStreamDone
        )
    }
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
            0x36 => Ok(Self::RangeReadStreamRequest),
            0x37 => Ok(Self::RangeReadStreamChunk),
            0x38 => Ok(Self::RangeReadStreamHeartbeat),
            0x39 => Ok(Self::RangeReadStreamDone),
            0x3A => Ok(Self::RangeReadStreamCancel),
            0x27 => Ok(Self::TruncateForward),
            0x28 => Ok(Self::TruncateAck),
            0x29 => Ok(Self::RepairMerkleRequest),
            0x2A => Ok(Self::RepairMerkleResponse),
            0x2B => Ok(Self::RepairFetchRequest),
            0x2C => Ok(Self::RepairFetchResponse),
            0x2D => Ok(Self::RepairApplyRequest),
            0x2E => Ok(Self::RepairApplyResponse),
            0x30 => Ok(Self::StreamStart),
            0x31 => Ok(Self::StreamChunk),
            0x32 => Ok(Self::StreamEnd),
            0x33 => Ok(Self::SstableStreamStart),
            0x34 => Ok(Self::SstableStreamChunk),
            0x35 => Ok(Self::SstableStreamEnd),
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
            0x62 => Ok(Self::IndexReadRequest),
            0x63 => Ok(Self::IndexReadResponse),
            0x64 => Ok(Self::FulltextSearchRequest),
            0x65 => Ok(Self::FulltextSearchResponse),
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
            0x82 => Ok(Self::ClusterMembershipForward),
            0x83 => Ok(Self::ClusterMembershipForwardAck),
            _ => Err(NetError::UnknownMessageType(value)),
        }
    }
}

/// 32-byte trace context embedded in every internode frame header.
///
/// Layout: `trace_id(16) + span_id(8) + flags(8)`.
/// All zeros means no active trace context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceContext {
    /// 128-bit trace identifier.
    pub trace_id: [u8; 16],
    /// 64-bit span identifier.
    pub span_id: [u8; 8],
    /// 64-bit flags (e.g. sampled bit).
    pub flags: [u8; 8],
}

impl TraceContext {
    /// An empty trace context (no active trace).
    pub const EMPTY: Self = Self {
        trace_id: [0; 16],
        span_id: [0; 8],
        flags: [0; 8],
    };

    /// Returns true if this context carries no trace information.
    pub fn is_empty(&self) -> bool {
        self.trace_id == [0; 16] && self.span_id == [0; 8] && self.flags == [0; 8]
    }

    /// Encode the trace context into 32 bytes.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.trace_id);
        buf.put_slice(&self.span_id);
        buf.put_slice(&self.flags);
    }

    /// Decode 32 bytes into a trace context.
    pub fn decode(buf: &[u8]) -> Self {
        assert!(buf.len() >= TRACE_CONTEXT_SIZE, "trace context too short");
        let mut trace_id = [0u8; 16];
        trace_id.copy_from_slice(&buf[0..16]);
        let mut span_id = [0u8; 8];
        span_id.copy_from_slice(&buf[16..24]);
        let mut flags = [0u8; 8];
        flags.copy_from_slice(&buf[24..32]);
        Self {
            trace_id,
            span_id,
            flags,
        }
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::EMPTY
    }
}

/// 44-byte wire frame header (12 legacy + 32 trace context).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    pub version: u8,
    pub flags: u8,
    pub lane: Lane,
    pub msg_type: MsgType,
    pub stream_id: u32,
    pub length: u32,
    /// Distributed trace context propagated across internode RPCs.
    pub trace_context: TraceContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFrameFormat {
    Legacy,
    CapnpEnvelope,
}

impl WireFrameFormat {
    pub fn version(self) -> u8 {
        match self {
            Self::Legacy => LEGACY_FRAME_VERSION,
            Self::CapnpEnvelope => CAPNP_FRAME_VERSION,
        }
    }
}

impl FrameHeader {
    /// Create a new legacy frame header with flags=0 and empty trace context.
    pub fn new(msg_type: MsgType, lane: Lane, stream_id: u32, length: u32) -> Self {
        Self::new_with_format(WireFrameFormat::Legacy, msg_type, lane, stream_id, length)
    }

    pub fn new_with_format(
        format: WireFrameFormat,
        msg_type: MsgType,
        lane: Lane,
        stream_id: u32,
        length: u32,
    ) -> Self {
        Self {
            version: format.version(),
            flags: 0,
            lane,
            msg_type,
            stream_id,
            length,
            trace_context: TraceContext::EMPTY,
        }
    }

    /// Create a new frame header with the given trace context.
    pub fn with_trace(
        msg_type: MsgType,
        lane: Lane,
        stream_id: u32,
        length: u32,
        trace_context: TraceContext,
    ) -> Self {
        Self {
            version: 1,
            flags: 0,
            lane,
            msg_type,
            stream_id,
            length,
            trace_context,
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u8(self.flags);
        buf.put_u8(self.lane as u8);
        buf.put_u8(self.msg_type as u8);
        buf.put_u32(self.stream_id);
        buf.put_u32(self.length);
        self.trace_context.encode(buf);
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < HEADER_SIZE {
            return Err(NetError::Protocol("header too short".into()));
        }
        let version = buf[0];
        if version != LEGACY_FRAME_VERSION && version != CAPNP_FRAME_VERSION {
            return Err(NetError::Protocol(format!(
                "unsupported internode protocol version: {version}"
            )));
        }
        let trace_context = TraceContext::decode(&buf[12..44]);
        Ok(Self {
            version,
            flags: buf[1],
            lane: Lane::try_from(buf[2])?,
            msg_type: MsgType::try_from(buf[3])?,
            stream_id: u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]),
            length: u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]),
            trace_context,
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
    frame_format: WireFrameFormat,
}

impl InternodeCodec {
    pub fn new(max_frame_body_size: u32) -> Self {
        Self::with_format(max_frame_body_size, WireFrameFormat::Legacy)
    }

    pub fn with_format(max_frame_body_size: u32, frame_format: WireFrameFormat) -> Self {
        Self {
            max_frame_body_size,
            frame_format,
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
        if header.version != self.frame_format.version() {
            let reason = match (self.frame_format, header.version) {
                (WireFrameFormat::CapnpEnvelope, LEGACY_FRAME_VERSION) => {
                    "legacy frame received on CapnProto envelope connection".to_string()
                }
                (WireFrameFormat::Legacy, CAPNP_FRAME_VERSION) => {
                    "CapnProto envelope frame received on legacy connection".to_string()
                }
                (expected, actual) => format!(
                    "internode frame version {actual} does not match negotiated format {expected:?}"
                ),
            };
            return Err(NetError::Protocol(reason));
        }
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
        header.version = self.frame_format.version();
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
            trace_context: TraceContext::EMPTY,
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
        assert!(decoded.trace_context.is_empty());
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
            trace_context: TraceContext::EMPTY,
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
            trace_context: TraceContext::EMPTY,
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
            trace_context: TraceContext::EMPTY,
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

    /// Every anti-entropy repair RPC byte-tag must round-trip through
    /// `TryFrom<u8>`. The enum has variants 0x29..=0x2E (declared at the
    /// top of this module) but those cases were not added to the decoder's
    /// match arm — so peers serialise repair frames fine but reject them
    /// on receipt with `UnknownMessageType`, repair sessions silently
    /// fail and zero partitions converge. This test pins each byte.
    #[test]
    fn msg_type_repair_rpc_tags_round_trip() {
        for (byte, expected) in [
            (0x29u8, MsgType::RepairMerkleRequest),
            (0x2A, MsgType::RepairMerkleResponse),
            (0x2B, MsgType::RepairFetchRequest),
            (0x2C, MsgType::RepairFetchResponse),
            (0x2D, MsgType::RepairApplyRequest),
            (0x2E, MsgType::RepairApplyResponse),
        ] {
            let parsed = MsgType::try_from(byte).unwrap_or_else(|_| {
                panic!("MsgType::try_from(0x{byte:02X}) must succeed — repair will silently drop frames otherwise")
            });
            assert_eq!(parsed, expected, "byte 0x{byte:02X} round-trip");
            assert_eq!(parsed as u8, byte, "byte 0x{byte:02X} discriminant");
        }
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
                trace_context: TraceContext::EMPTY,
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

    #[test]
    fn trace_context_roundtrip() {
        let ctx = TraceContext {
            trace_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            span_id: [0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44],
            flags: [0x01, 0, 0, 0, 0, 0, 0, 0],
        };
        let mut buf = BytesMut::new();
        ctx.encode(&mut buf);
        assert_eq!(buf.len(), TRACE_CONTEXT_SIZE);
        let decoded = TraceContext::decode(&buf);
        assert_eq!(decoded, ctx);
        assert!(!decoded.is_empty());
    }

    #[test]
    fn trace_context_empty_is_all_zeros() {
        let ctx = TraceContext::EMPTY;
        assert!(ctx.is_empty());
    }

    #[test]
    fn trace_context_propagated_across_rpc() {
        let trace_ctx = TraceContext {
            trace_id: [1; 16],
            span_id: [2; 8],
            flags: [0x01, 0, 0, 0, 0, 0, 0, 0],
        };
        let header = FrameHeader::with_trace(MsgType::MutationForward, Lane::Data, 7, 0, trace_ctx);
        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        let decoded = FrameHeader::decode(&buf).unwrap();
        assert_eq!(decoded.trace_context.trace_id, [1; 16]);
        assert_eq!(decoded.trace_context.span_id, [2; 8]);
        assert_eq!(decoded.trace_context.flags[0], 0x01);
        assert!(!decoded.trace_context.is_empty());
    }

    /// ADR-020 streaming range-read variants must round-trip through
    /// the u8 wire encoding. New byte slots: 0x36..=0x3A. These are
    /// the dedicated multi-message types — they coexist with the
    /// legacy single-shot RangeReadRequest/RangeReadResponse (0x25,
    /// 0x26) so a rolling upgrade can negotiate the protocol version
    /// without breaking older peers.
    #[test]
    fn msg_type_round_trip_for_streaming_range_read_variants() {
        for (byte, expected) in [
            (0x36u8, MsgType::RangeReadStreamRequest),
            (0x37, MsgType::RangeReadStreamChunk),
            (0x38, MsgType::RangeReadStreamHeartbeat),
            (0x39, MsgType::RangeReadStreamDone),
            (0x3A, MsgType::RangeReadStreamCancel),
        ] {
            let parsed = MsgType::try_from(byte).expect("known streaming MsgType byte");
            assert_eq!(parsed, expected, "byte 0x{byte:02X} did not decode");
            assert_eq!(parsed as u8, byte, "round-trip byte mismatch");
        }
    }
}
