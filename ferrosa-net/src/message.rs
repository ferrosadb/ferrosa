// ferrosa-net/src/message.rs
use bytes::{Buf, BufMut, Bytes, BytesMut};
use uuid::Uuid;

use crate::codec::MsgType;
use crate::error::{NetError, Result};

fn put_string(buf: &mut BytesMut, s: &str) -> Result<()> {
    let len = u16::try_from(s.len())
        .map_err(|_| NetError::Protocol("string exceeds u16::MAX bytes".into()))?;
    buf.put_u16(len);
    buf.put_slice(s.as_bytes());
    Ok(())
}

fn get_string(buf: &mut Bytes) -> Result<String> {
    if buf.remaining() < 2 {
        return Err(NetError::Protocol("truncated string length".into()));
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(NetError::Protocol("truncated string body".into()));
    }
    let bytes = buf.split_to(len);
    String::from_utf8(bytes.to_vec())
        .map_err(|_| NetError::Protocol("invalid UTF-8 in string".into()))
}

fn put_uuid(buf: &mut BytesMut, id: &Uuid) {
    buf.put_slice(id.as_bytes());
}

fn get_uuid(buf: &mut Bytes) -> Result<Uuid> {
    if buf.remaining() < 16 {
        return Err(NetError::Protocol("truncated UUID".into()));
    }
    let mut bytes = [0u8; 16];
    buf.copy_to_slice(&mut bytes);
    Ok(Uuid::from_bytes(bytes))
}

fn put_bytes(buf: &mut BytesMut, data: &[u8]) -> Result<()> {
    let len = u32::try_from(data.len())
        .map_err(|_| NetError::Protocol("bytes field exceeds u32::MAX".into()))?;
    buf.put_u32(len);
    buf.put_slice(data);
    Ok(())
}

fn get_bytes(buf: &mut Bytes) -> Result<Vec<u8>> {
    if buf.remaining() < 4 {
        return Err(NetError::Protocol("truncated bytes length".into()));
    }
    let len = buf.get_u32() as usize;
    if buf.remaining() < len {
        return Err(NetError::Protocol("truncated bytes body".into()));
    }
    Ok(buf.split_to(len).to_vec())
}

/// Internode message. Each variant corresponds to a MsgType.
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    // Lifecycle
    Handshake {
        cluster_name: String,
        host_id: Uuid,
        protocol_version: u8,
        supported_compression: Vec<u8>, // 0=none, 1=lz4, 2=snappy
        auth_token: Vec<u8>,
    },
    HandshakeAck {
        host_id: Uuid,
        protocol_version: u8,
        chosen_compression: u8, // selected from initiator's supported list
        accepted: bool,
        reason: String,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },

    // Raft — opaque payloads, ferrosa-cluster interprets
    RaftAppendEntries(Bytes),
    RaftAppendResponse(Bytes),
    RaftVote(Bytes),
    RaftVoteResponse(Bytes),
    RaftInstallSnapshot(Bytes),

    // Data — opaque payloads
    MutationForward(Bytes),
    MutationAck(Bytes),
    ReadRequest(Bytes),
    ReadResponse(Bytes),
    RepairWrite(Bytes),

    // Streaming — opaque payloads
    StreamStart(Bytes),
    StreamChunk(Bytes),
    StreamEnd(Bytes),

    // Pair mode
    PairWriteForward(Bytes),
    PairWriteAck(Bytes),
    PairCatchUp {
        last_segment_id: u64,
        last_offset: u32,
    },
    PairCatchUpResponse(Bytes),
    RoleSwap {
        new_primary: Uuid,
        new_secondary: Uuid,
    },
    /// Schema snapshot for catch-up sync (JSON-serialized SchemaSnapshot).
    PairSchemaSync(Bytes),
    /// DDL operation forwarded between pair nodes (JSON-serialized DdlOperation).
    PairDdlForward(Bytes),
    /// DDL acknowledgment.
    PairDdlAck(Bytes),

    // Batchlog
    /// Batchlog write request (serialized BatchlogEntry).
    BatchlogWrite(Bytes),
    /// Batchlog delete request (batch UUID).
    BatchlogDelete(Bytes),
    /// Batchlog replay request (serialized BatchlogEntry).
    BatchlogReplay(Bytes),
}

impl Message {
    pub fn msg_type(&self) -> MsgType {
        match self {
            Self::Handshake { .. } => MsgType::Handshake,
            Self::HandshakeAck { .. } => MsgType::HandshakeAck,
            Self::Ping { .. } => MsgType::Ping,
            Self::Pong { .. } => MsgType::Pong,
            Self::RaftAppendEntries(_) => MsgType::RaftAppendEntries,
            Self::RaftAppendResponse(_) => MsgType::RaftAppendResponse,
            Self::RaftVote(_) => MsgType::RaftVote,
            Self::RaftVoteResponse(_) => MsgType::RaftVoteResponse,
            Self::RaftInstallSnapshot(_) => MsgType::RaftInstallSnapshot,
            Self::MutationForward(_) => MsgType::MutationForward,
            Self::MutationAck(_) => MsgType::MutationAck,
            Self::ReadRequest(_) => MsgType::ReadRequest,
            Self::ReadResponse(_) => MsgType::ReadResponse,
            Self::RepairWrite(_) => MsgType::RepairWrite,
            Self::StreamStart(_) => MsgType::StreamStart,
            Self::StreamChunk(_) => MsgType::StreamChunk,
            Self::StreamEnd(_) => MsgType::StreamEnd,
            Self::PairWriteForward(_) => MsgType::PairWriteForward,
            Self::PairWriteAck(_) => MsgType::PairWriteAck,
            Self::PairCatchUp { .. } => MsgType::PairCatchUp,
            Self::PairCatchUpResponse(_) => MsgType::PairCatchUpResponse,
            Self::RoleSwap { .. } => MsgType::RoleSwap,
            Self::PairSchemaSync(_) => MsgType::PairSchemaSync,
            Self::PairDdlForward(_) => MsgType::PairDdlForward,
            Self::PairDdlAck(_) => MsgType::PairDdlAck,
            Self::BatchlogWrite(_) => MsgType::BatchlogWrite,
            Self::BatchlogDelete(_) => MsgType::BatchlogDelete,
            Self::BatchlogReplay(_) => MsgType::BatchlogReplay,
        }
    }

    /// Encode message body to bytes. Does NOT include the frame header.
    pub fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            Self::Handshake {
                cluster_name,
                host_id,
                protocol_version,
                supported_compression,
                auth_token,
            } => {
                put_string(buf, cluster_name)?;
                put_uuid(buf, host_id);
                buf.put_u8(*protocol_version);
                let comp_len = u8::try_from(supported_compression.len())
                    .map_err(|_| NetError::Protocol("compression list exceeds 255".into()))?;
                buf.put_u8(comp_len);
                buf.put_slice(supported_compression);
                put_bytes(buf, auth_token)?;
            }
            Self::HandshakeAck {
                host_id,
                protocol_version,
                chosen_compression,
                accepted,
                reason,
            } => {
                put_uuid(buf, host_id);
                buf.put_u8(*protocol_version);
                buf.put_u8(*chosen_compression);
                buf.put_u8(if *accepted { 1 } else { 0 });
                put_string(buf, reason)?;
            }
            Self::Ping { nonce } | Self::Pong { nonce } => buf.put_u64(*nonce),
            Self::PairCatchUp {
                last_segment_id,
                last_offset,
            } => {
                buf.put_u64(*last_segment_id);
                buf.put_u32(*last_offset);
            }
            Self::RoleSwap {
                new_primary,
                new_secondary,
            } => {
                put_uuid(buf, new_primary);
                put_uuid(buf, new_secondary);
            }
            // Opaque payload variants — copy body directly, no additional framing
            Self::RaftAppendEntries(b)
            | Self::RaftAppendResponse(b)
            | Self::RaftVote(b)
            | Self::RaftVoteResponse(b)
            | Self::RaftInstallSnapshot(b)
            | Self::MutationForward(b)
            | Self::MutationAck(b)
            | Self::ReadRequest(b)
            | Self::ReadResponse(b)
            | Self::RepairWrite(b)
            | Self::StreamStart(b)
            | Self::StreamChunk(b)
            | Self::StreamEnd(b)
            | Self::PairWriteForward(b)
            | Self::PairWriteAck(b)
            | Self::PairCatchUpResponse(b)
            | Self::PairSchemaSync(b)
            | Self::PairDdlForward(b)
            | Self::PairDdlAck(b)
            | Self::BatchlogWrite(b)
            | Self::BatchlogDelete(b)
            | Self::BatchlogReplay(b) => buf.put_slice(b),
        }
        Ok(())
    }

    /// Decode message body from bytes given the message type from the frame header.
    pub fn decode(msg_type: MsgType, body: &mut Bytes) -> Result<Self> {
        Ok(match msg_type {
            MsgType::Handshake => {
                let cluster_name = get_string(body)?;
                let host_id = get_uuid(body)?;
                if body.remaining() < 1 {
                    return Err(NetError::Protocol("truncated handshake".into()));
                }
                let protocol_version = body.get_u8();
                if body.remaining() < 1 {
                    return Err(NetError::Protocol("truncated compression list".into()));
                }
                let comp_len = body.get_u8() as usize;
                if body.remaining() < comp_len {
                    return Err(NetError::Protocol("truncated compression".into()));
                }
                let supported_compression = body.split_to(comp_len).to_vec();
                let auth_token = get_bytes(body)?;
                Self::Handshake {
                    cluster_name,
                    host_id,
                    protocol_version,
                    supported_compression,
                    auth_token,
                }
            }
            MsgType::HandshakeAck => {
                let host_id = get_uuid(body)?;
                if body.remaining() < 3 {
                    return Err(NetError::Protocol("truncated handshake ack".into()));
                }
                let protocol_version = body.get_u8();
                let chosen_compression = body.get_u8();
                let accepted = body.get_u8() != 0;
                let reason = get_string(body)?;
                Self::HandshakeAck {
                    host_id,
                    protocol_version,
                    chosen_compression,
                    accepted,
                    reason,
                }
            }
            MsgType::Ping => {
                if body.remaining() < 8 {
                    return Err(NetError::Protocol("truncated ping".into()));
                }
                Self::Ping {
                    nonce: body.get_u64(),
                }
            }
            MsgType::Pong => {
                if body.remaining() < 8 {
                    return Err(NetError::Protocol("truncated pong".into()));
                }
                Self::Pong {
                    nonce: body.get_u64(),
                }
            }
            MsgType::PairCatchUp => {
                if body.remaining() < 12 {
                    return Err(NetError::Protocol("truncated pair catch up".into()));
                }
                Self::PairCatchUp {
                    last_segment_id: body.get_u64(),
                    last_offset: body.get_u32(),
                }
            }
            MsgType::RoleSwap => {
                let new_primary = get_uuid(body)?;
                let new_secondary = get_uuid(body)?;
                Self::RoleSwap {
                    new_primary,
                    new_secondary,
                }
            }
            // Opaque payload variants — take remaining bytes as payload
            MsgType::RaftAppendEntries => Self::RaftAppendEntries(body.split_to(body.remaining())),
            MsgType::RaftAppendResponse => {
                Self::RaftAppendResponse(body.split_to(body.remaining()))
            }
            MsgType::RaftVote => Self::RaftVote(body.split_to(body.remaining())),
            MsgType::RaftVoteResponse => Self::RaftVoteResponse(body.split_to(body.remaining())),
            MsgType::RaftInstallSnapshot => {
                Self::RaftInstallSnapshot(body.split_to(body.remaining()))
            }
            MsgType::MutationForward => Self::MutationForward(body.split_to(body.remaining())),
            MsgType::MutationAck => Self::MutationAck(body.split_to(body.remaining())),
            MsgType::ReadRequest => Self::ReadRequest(body.split_to(body.remaining())),
            MsgType::ReadResponse => Self::ReadResponse(body.split_to(body.remaining())),
            MsgType::RepairWrite => Self::RepairWrite(body.split_to(body.remaining())),
            MsgType::StreamStart => Self::StreamStart(body.split_to(body.remaining())),
            MsgType::StreamChunk => Self::StreamChunk(body.split_to(body.remaining())),
            MsgType::StreamEnd => Self::StreamEnd(body.split_to(body.remaining())),
            MsgType::PairWriteForward => Self::PairWriteForward(body.split_to(body.remaining())),
            MsgType::PairWriteAck => Self::PairWriteAck(body.split_to(body.remaining())),
            MsgType::PairCatchUpResponse => {
                Self::PairCatchUpResponse(body.split_to(body.remaining()))
            }
            MsgType::PairSchemaSync => Self::PairSchemaSync(body.split_to(body.remaining())),
            MsgType::PairDdlForward => Self::PairDdlForward(body.split_to(body.remaining())),
            MsgType::PairDdlAck => Self::PairDdlAck(body.split_to(body.remaining())),
            MsgType::BatchlogWrite => Self::BatchlogWrite(body.split_to(body.remaining())),
            MsgType::BatchlogDelete => Self::BatchlogDelete(body.split_to(body.remaining())),
            MsgType::BatchlogReplay => Self::BatchlogReplay(body.split_to(body.remaining())),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn ping_roundtrip() {
        let msg = Message::Ping { nonce: 42 };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::Ping, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, Message::Ping { nonce: 42 });
    }

    #[test]
    fn handshake_roundtrip() {
        let msg = Message::Handshake {
            cluster_name: "ferrosa".to_string(),
            host_id: Uuid::new_v4(),
            protocol_version: 1,
            supported_compression: vec![0, 1], // none + lz4
            auth_token: vec![0xAB; 32],
        };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::Handshake, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn pair_catch_up_roundtrip() {
        let msg = Message::PairCatchUp {
            last_segment_id: 17,
            last_offset: 4096,
        };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::PairCatchUp, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn repair_write_roundtrip() {
        let payload = Bytes::from_static(b"repair-data");
        let msg = Message::RepairWrite(payload.clone());
        assert_eq!(msg.msg_type(), MsgType::RepairWrite);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::RepairWrite, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, Message::RepairWrite(payload));
    }

    #[test]
    fn batchlog_write_roundtrip() {
        let payload = Bytes::from_static(b"test-batchlog-entry");
        let msg = Message::BatchlogWrite(payload.clone());
        assert_eq!(msg.msg_type(), MsgType::BatchlogWrite);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::BatchlogWrite, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, Message::BatchlogWrite(payload));
    }

    #[test]
    fn batchlog_delete_roundtrip() {
        let payload = Bytes::from_static(b"batch-uuid-bytes");
        let msg = Message::BatchlogDelete(payload.clone());
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::BatchlogDelete, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, Message::BatchlogDelete(payload));
    }

    #[test]
    fn batchlog_replay_roundtrip() {
        let payload = Bytes::from_static(b"replay-data");
        let msg = Message::BatchlogReplay(payload.clone());
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::BatchlogReplay, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, Message::BatchlogReplay(payload));
    }

    #[test]
    fn batchlog_msg_types_from_u8() {
        assert_eq!(MsgType::try_from(0x50).unwrap(), MsgType::BatchlogWrite);
        assert_eq!(MsgType::try_from(0x51).unwrap(), MsgType::BatchlogDelete);
        assert_eq!(MsgType::try_from(0x52).unwrap(), MsgType::BatchlogReplay);
    }

    proptest! {
        #[test]
        fn decode_never_panics(data in proptest::collection::vec(any::<u8>(), 0..512)) {
            let bytes = Bytes::from(data);
            // Try decoding as each message type — should return Ok or Err, never panic
            for msg_type_byte in 0x01..=0x52u8 {
                if let Ok(msg_type) = MsgType::try_from(msg_type_byte) {
                    let _ = Message::decode(msg_type, &mut bytes.clone());
                }
            }
        }
    }
}
