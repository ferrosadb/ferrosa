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

/// Encode an optional, length-prefixed string as a presence marker byte
/// (`1` = present, `0` = absent) followed by the string when present.
fn put_optional_string(buf: &mut BytesMut, value: &Option<String>) -> Result<()> {
    match value {
        Some(s) => {
            buf.put_u8(1);
            put_string(buf, s)?;
        }
        None => {
            buf.put_u8(0);
        }
    }
    Ok(())
}

/// Decode an optional, length-prefixed string written by [`put_optional_string`].
///
/// Returns `None` when the buffer is exhausted (a pre-extension peer that did
/// not write this trailing field), preserving backward compatibility.
fn get_optional_string(buf: &mut Bytes) -> Result<Option<String>> {
    if buf.remaining() == 0 {
        return Ok(None);
    }
    let marker = buf.get_u8();
    if marker == 1 {
        Ok(Some(get_string(buf)?))
    } else {
        Ok(None)
    }
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
        cql_broadcast: Option<String>,
        /// Raw internode broadcast hostname:port (re-resolvable). Peers store
        /// this in `NodeInfo.addr` so they can re-resolve it across IP churn.
        internode_broadcast: Option<String>,
    },
    HandshakeAck {
        host_id: Uuid,
        protocol_version: u8,
        chosen_compression: u8, // selected from initiator's supported list
        accepted: bool,
        reason: String,
        cql_broadcast: Option<String>,
        /// Raw internode broadcast hostname:port (re-resolvable). Peers store
        /// this in `NodeInfo.addr` so they can re-resolve it across IP churn.
        internode_broadcast: Option<String>,
    },
    Ping {
        nonce: u64,
        /// Sender's wall-clock time in nanoseconds when this Ping was sent.
        sent_at: u64,
    },
    Pong {
        nonce: u64,
        /// Wall-clock time (ns) when the Ping was received by the responder.
        ping_recv_at: u64,
        /// Wall-clock time (ns) when this Pong was sent by the responder.
        sent_at: u64,
    },

    // Cluster formation
    /// Invitation to join the cluster, carrying the full known peer list.
    ClusterInvite {
        initiator: Uuid,
        peers: Vec<(Uuid, std::net::SocketAddr)>,
    },
    /// Acknowledgment of a ClusterInvite.
    ClusterInviteAck {
        host_id: Uuid,
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
    RangeReadRequest(Bytes),
    RangeReadResponse(Bytes),
    /// ADR-020: client side of the streaming range-read RPC. Payload
    /// is a bincoded `RangeReadStreamRequestPayload` (defined in
    /// ferrosa-cluster) carrying `request_id`, keyspace/table, and
    /// optional start/end key bounds.
    RangeReadStreamRequest(Bytes),
    /// ADR-020: one batch of partitions belonging to a streaming
    /// range-read response, keyed by `request_id`. Payload is a
    /// bincoded `RangeReadStreamChunkPayload`.
    RangeReadStreamChunk(Bytes),
    /// ADR-020: keep-alive emitted by the handler when the next chunk
    /// is slow to produce (e.g., S3 fetch, compaction back-pressure).
    /// The coordinator's idle watchdog treats heartbeats as activity.
    RangeReadStreamHeartbeat(Bytes),
    /// ADR-020: terminator for a streaming range-read response.
    /// Payload carries final stream metadata (total_chunks, truncated
    /// flag).
    RangeReadStreamDone(Bytes),
    /// ADR-020: coordinator → handler signal to abort a stream
    /// in-flight (CQL client disconnected, read-quorum already
    /// satisfied, KILL issued). Handler stops iterating between
    /// batches.
    RangeReadStreamCancel(Bytes),
    TruncateForward(Bytes),
    TruncateAck(Bytes),

    /// Anti-entropy repair — initiator → peer: "build a Merkle tree for
    /// keyspace.table over the given token range and send it back".
    /// Payload is a bincoded `RepairMerkleRequestPayload`.
    RepairMerkleRequest(Bytes),
    /// Peer → initiator: serialized `MerkleTree`.
    RepairMerkleResponse(Bytes),
    /// Anti-entropy repair — initiator → peer: "send me the partitions
    /// in this token sub-range". Payload is a bincoded
    /// `RepairFetchRequestPayload`.
    RepairFetchRequest(Bytes),
    /// Peer → initiator: bincoded `RepairFetchResponsePayload` carrying
    /// the matching partitions (as `PartitionWire` for serde).
    RepairFetchResponse(Bytes),
    /// Anti-entropy repair — initiator → peer: "apply these partitions
    /// last-write-wins on a per-cell basis". Payload is a bincoded
    /// `RepairApplyRequestPayload`.
    RepairApplyRequest(Bytes),
    /// Peer → initiator: bincoded `RepairApplyResponsePayload` carrying
    /// the count of partitions applied and an optional error message.
    RepairApplyResponse(Bytes),

    // Streaming (row-based) — opaque payloads
    StreamStart(Bytes),
    StreamChunk(Bytes),
    StreamEnd(Bytes),
    // Streaming (SSTable file-based) — opaque payloads
    SstableStreamStart(Bytes),
    SstableStreamChunk(Bytes),
    SstableStreamEnd(Bytes),

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
    /// Atomic batch forwarded between pair nodes.
    ///
    /// Payload layout: `batch_id:[u8;16] | mutation_count:u32 | mutations…`
    /// where each mutation is serialized with a 4-byte (u32 BE) length prefix.
    PairBatchForward(Bytes),
    /// Acknowledgment for a [`PairBatchForward`](Self::PairBatchForward).
    PairBatchAck(Bytes),

    // Batchlog
    /// Batchlog write request (serialized BatchlogEntry).
    BatchlogWrite(Bytes),
    /// Batchlog delete request (batch UUID).
    BatchlogDelete(Bytes),
    /// Batchlog replay request (serialized BatchlogEntry).
    BatchlogReplay(Bytes),

    // Index build coordination -- opaque payloads
    IndexBuildRequest(Bytes),
    IndexBuildComplete(Bytes),

    // Secondary index scatter-gather
    IndexReadRequest(Bytes),
    IndexReadResponse(Bytes),

    // Accord consensus — opaque payloads, ferrosa-cluster interprets
    AccordPreAccept(Bytes),
    AccordPreAcceptOK(Bytes),
    AccordAccept(Bytes),
    AccordAcceptOK(Bytes),
    AccordCommit(Bytes),
    AccordRead(Bytes),
    AccordReadOK(Bytes),
    AccordApply(Bytes),
    AccordApplyOK(Bytes),
    AccordRecover(Bytes),
    AccordRecoverOK(Bytes),

    // Bootstrap coordination
    /// Sent by a non-leader node to the leader after bootstrap streaming completes.
    /// Leader waits for this from all joining nodes before promoting them to Normal.
    BootstrapComplete {
        /// The sending node's host_id.
        node_id: uuid::Uuid,
    },
    /// Leader acknowledges receipt of BootstrapComplete.
    BootstrapCompleteAck,

    // Cluster-mode membership proposal forwarding
    /// Forward a typed `MembershipOp` to the current Raft leader (W1.13).
    ///
    /// Renamed from `ClusterRaftForward` (Sprint 1) to make the scope
    /// explicit: the body is no longer an opaque bincoded `RaftCommand`
    /// but a `MembershipOp` enum (defined in
    /// `ferrosa_cluster::membership::wire`).  The leader dispatches on
    /// the operation tag rather than peeking inside.
    ///
    /// Used by non-leader nodes that hit openraft's `ForwardToLeader`
    /// hint when their local `client_write` is rejected.  The leader
    /// applies the operation via `MembershipChanger` and replies with
    /// a [`Self::ClusterMembershipForwardAck`].
    ///
    /// Wire byte unchanged (`0x82`) so a workspace-coherent rolling
    /// upgrade does not need a wire-format flag day; only the payload
    /// schema changes.
    ClusterMembershipForward(Bytes),
    /// Acknowledgment for a [`Self::ClusterMembershipForward`].
    ///
    /// Payload: bincode-serialized status enum.  Empty bytes indicates
    /// success on legacy senders.
    ClusterMembershipForwardAck(Bytes),
}

impl Message {
    pub fn msg_type(&self) -> MsgType {
        match self {
            Self::Handshake { .. } => MsgType::Handshake,
            Self::HandshakeAck { .. } => MsgType::HandshakeAck,
            Self::Ping { .. } => MsgType::Ping,
            Self::Pong { .. } => MsgType::Pong,
            Self::ClusterInvite { .. } => MsgType::ClusterInvite,
            Self::ClusterInviteAck { .. } => MsgType::ClusterInviteAck,
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
            Self::RangeReadRequest(_) => MsgType::RangeReadRequest,
            Self::RangeReadResponse(_) => MsgType::RangeReadResponse,
            Self::RangeReadStreamRequest(_) => MsgType::RangeReadStreamRequest,
            Self::RangeReadStreamChunk(_) => MsgType::RangeReadStreamChunk,
            Self::RangeReadStreamHeartbeat(_) => MsgType::RangeReadStreamHeartbeat,
            Self::RangeReadStreamDone(_) => MsgType::RangeReadStreamDone,
            Self::RangeReadStreamCancel(_) => MsgType::RangeReadStreamCancel,
            Self::TruncateForward(_) => MsgType::TruncateForward,
            Self::TruncateAck(_) => MsgType::TruncateAck,
            Self::RepairMerkleRequest(_) => MsgType::RepairMerkleRequest,
            Self::RepairMerkleResponse(_) => MsgType::RepairMerkleResponse,
            Self::RepairFetchRequest(_) => MsgType::RepairFetchRequest,
            Self::RepairFetchResponse(_) => MsgType::RepairFetchResponse,
            Self::RepairApplyRequest(_) => MsgType::RepairApplyRequest,
            Self::RepairApplyResponse(_) => MsgType::RepairApplyResponse,
            Self::StreamStart(_) => MsgType::StreamStart,
            Self::StreamChunk(_) => MsgType::StreamChunk,
            Self::StreamEnd(_) => MsgType::StreamEnd,
            Self::SstableStreamStart(_) => MsgType::SstableStreamStart,
            Self::SstableStreamChunk(_) => MsgType::SstableStreamChunk,
            Self::SstableStreamEnd(_) => MsgType::SstableStreamEnd,
            Self::PairWriteForward(_) => MsgType::PairWriteForward,
            Self::PairWriteAck(_) => MsgType::PairWriteAck,
            Self::PairCatchUp { .. } => MsgType::PairCatchUp,
            Self::PairCatchUpResponse(_) => MsgType::PairCatchUpResponse,
            Self::RoleSwap { .. } => MsgType::RoleSwap,
            Self::PairSchemaSync(_) => MsgType::PairSchemaSync,
            Self::PairDdlForward(_) => MsgType::PairDdlForward,
            Self::PairDdlAck(_) => MsgType::PairDdlAck,
            Self::PairBatchForward(_) => MsgType::PairBatchForward,
            Self::PairBatchAck(_) => MsgType::PairBatchAck,
            Self::BatchlogWrite(_) => MsgType::BatchlogWrite,
            Self::BatchlogDelete(_) => MsgType::BatchlogDelete,
            Self::BatchlogReplay(_) => MsgType::BatchlogReplay,
            Self::IndexBuildRequest(_) => MsgType::IndexBuildRequest,
            Self::IndexBuildComplete(_) => MsgType::IndexBuildComplete,
            Self::IndexReadRequest(_) => MsgType::IndexReadRequest,
            Self::IndexReadResponse(_) => MsgType::IndexReadResponse,
            Self::AccordPreAccept(_) => MsgType::AccordPreAccept,
            Self::AccordPreAcceptOK(_) => MsgType::AccordPreAcceptOK,
            Self::AccordAccept(_) => MsgType::AccordAccept,
            Self::AccordAcceptOK(_) => MsgType::AccordAcceptOK,
            Self::AccordCommit(_) => MsgType::AccordCommit,
            Self::AccordRead(_) => MsgType::AccordRead,
            Self::AccordReadOK(_) => MsgType::AccordReadOK,
            Self::AccordApply(_) => MsgType::AccordApply,
            Self::AccordApplyOK(_) => MsgType::AccordApplyOK,
            Self::AccordRecover(_) => MsgType::AccordRecover,
            Self::AccordRecoverOK(_) => MsgType::AccordRecoverOK,
            Self::BootstrapComplete { .. } => MsgType::BootstrapComplete,
            Self::BootstrapCompleteAck => MsgType::BootstrapCompleteAck,
            Self::ClusterMembershipForward(_) => MsgType::ClusterMembershipForward,
            Self::ClusterMembershipForwardAck(_) => MsgType::ClusterMembershipForwardAck,
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
                cql_broadcast,
                internode_broadcast,
            } => {
                put_string(buf, cluster_name)?;
                put_uuid(buf, host_id);
                buf.put_u8(*protocol_version);
                let comp_len = u8::try_from(supported_compression.len())
                    .map_err(|_| NetError::Protocol("compression list exceeds 255".into()))?;
                buf.put_u8(comp_len);
                buf.put_slice(supported_compression);
                put_bytes(buf, auth_token)?;
                // Optional CQL broadcast address (v1 extension — safe to add after auth_token)
                put_optional_string(buf, cql_broadcast)?;
                // Optional internode broadcast hostname (v1 extension — appended
                // after cql_broadcast; absent on pre-extension peers)
                put_optional_string(buf, internode_broadcast)?;
            }
            Self::HandshakeAck {
                host_id,
                protocol_version,
                chosen_compression,
                accepted,
                reason,
                cql_broadcast,
                internode_broadcast,
            } => {
                put_uuid(buf, host_id);
                buf.put_u8(*protocol_version);
                buf.put_u8(*chosen_compression);
                buf.put_u8(if *accepted { 1 } else { 0 });
                put_string(buf, reason)?;
                // Optional CQL broadcast address (v1 extension)
                put_optional_string(buf, cql_broadcast)?;
                // Optional internode broadcast hostname (v1 extension)
                put_optional_string(buf, internode_broadcast)?;
            }
            Self::Ping { nonce, sent_at } => {
                buf.put_u64(*nonce);
                buf.put_u64(*sent_at);
            }
            Self::Pong {
                nonce,
                ping_recv_at,
                sent_at,
            } => {
                buf.put_u64(*nonce);
                buf.put_u64(*ping_recv_at);
                buf.put_u64(*sent_at);
            }
            Self::ClusterInvite { initiator, peers } => {
                put_uuid(buf, initiator);
                let count = u32::try_from(peers.len()).map_err(|_| {
                    NetError::Protocol("cluster invite peer count exceeds u32::MAX".into())
                })?;
                buf.put_u32(count);
                for (id, addr) in peers {
                    put_uuid(buf, id);
                    put_string(buf, &addr.to_string())?;
                }
            }
            Self::ClusterInviteAck { host_id } => {
                put_uuid(buf, host_id);
            }
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
            | Self::RangeReadRequest(b)
            | Self::RangeReadResponse(b)
            | Self::RangeReadStreamRequest(b)
            | Self::RangeReadStreamChunk(b)
            | Self::RangeReadStreamHeartbeat(b)
            | Self::RangeReadStreamDone(b)
            | Self::RangeReadStreamCancel(b)
            | Self::TruncateForward(b)
            | Self::TruncateAck(b)
            | Self::RepairMerkleRequest(b)
            | Self::RepairMerkleResponse(b)
            | Self::RepairFetchRequest(b)
            | Self::RepairFetchResponse(b)
            | Self::RepairApplyRequest(b)
            | Self::RepairApplyResponse(b)
            | Self::StreamStart(b)
            | Self::StreamChunk(b)
            | Self::StreamEnd(b)
            | Self::SstableStreamStart(b)
            | Self::SstableStreamChunk(b)
            | Self::SstableStreamEnd(b)
            | Self::PairWriteForward(b)
            | Self::PairWriteAck(b)
            | Self::PairCatchUpResponse(b)
            | Self::PairSchemaSync(b)
            | Self::PairDdlForward(b)
            | Self::PairDdlAck(b)
            | Self::PairBatchForward(b)
            | Self::PairBatchAck(b)
            | Self::BatchlogWrite(b)
            | Self::BatchlogDelete(b)
            | Self::BatchlogReplay(b)
            | Self::IndexBuildRequest(b)
            | Self::IndexBuildComplete(b)
            | Self::IndexReadRequest(b)
            | Self::IndexReadResponse(b)
            | Self::AccordPreAccept(b)
            | Self::AccordPreAcceptOK(b)
            | Self::AccordAccept(b)
            | Self::AccordAcceptOK(b)
            | Self::AccordCommit(b)
            | Self::AccordRead(b)
            | Self::AccordReadOK(b)
            | Self::AccordApply(b)
            | Self::AccordApplyOK(b)
            | Self::AccordRecover(b)
            | Self::AccordRecoverOK(b)
            | Self::ClusterMembershipForward(b)
            | Self::ClusterMembershipForwardAck(b) => buf.put_slice(b),
            Self::BootstrapComplete { node_id } => buf.put_slice(node_id.as_bytes()),
            Self::BootstrapCompleteAck => {} // no payload
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
                // Optional CQL broadcast address (v1 extension)
                let cql_broadcast = get_optional_string(body)?;
                // Optional internode broadcast hostname (v1 extension)
                let internode_broadcast = get_optional_string(body)?;
                Self::Handshake {
                    cluster_name,
                    host_id,
                    protocol_version,
                    supported_compression,
                    auth_token,
                    cql_broadcast,
                    internode_broadcast,
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
                // Optional CQL broadcast address (v1 extension)
                let cql_broadcast = get_optional_string(body)?;
                // Optional internode broadcast hostname (v1 extension)
                let internode_broadcast = get_optional_string(body)?;
                Self::HandshakeAck {
                    host_id,
                    protocol_version,
                    chosen_compression,
                    accepted,
                    reason,
                    cql_broadcast,
                    internode_broadcast,
                }
            }
            MsgType::Ping => {
                if body.remaining() < 16 {
                    return Err(NetError::Protocol("truncated ping".into()));
                }
                Self::Ping {
                    nonce: body.get_u64(),
                    sent_at: body.get_u64(),
                }
            }
            MsgType::Pong => {
                if body.remaining() < 24 {
                    return Err(NetError::Protocol("truncated pong".into()));
                }
                Self::Pong {
                    nonce: body.get_u64(),
                    ping_recv_at: body.get_u64(),
                    sent_at: body.get_u64(),
                }
            }
            MsgType::ClusterInvite => {
                let initiator = get_uuid(body)?;
                if body.remaining() < 4 {
                    return Err(NetError::Protocol(
                        "truncated cluster invite peer count".into(),
                    ));
                }
                let count = body.get_u32() as usize;
                if count > 10_000 {
                    return Err(NetError::Protocol(format!(
                        "cluster invite peer count too large: {count}"
                    )));
                }
                let mut peers = Vec::with_capacity(count);
                for _ in 0..count {
                    let id = get_uuid(body)?;
                    let addr_str = get_string(body)?;
                    let addr: std::net::SocketAddr = addr_str.parse().map_err(|_| {
                        NetError::Protocol(format!("invalid socket address: {addr_str}"))
                    })?;
                    peers.push((id, addr));
                }
                Self::ClusterInvite { initiator, peers }
            }
            MsgType::ClusterInviteAck => {
                let host_id = get_uuid(body)?;
                Self::ClusterInviteAck { host_id }
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
            MsgType::RangeReadRequest => Self::RangeReadRequest(body.split_to(body.remaining())),
            MsgType::RangeReadResponse => Self::RangeReadResponse(body.split_to(body.remaining())),
            MsgType::RangeReadStreamRequest => {
                Self::RangeReadStreamRequest(body.split_to(body.remaining()))
            }
            MsgType::RangeReadStreamChunk => {
                Self::RangeReadStreamChunk(body.split_to(body.remaining()))
            }
            MsgType::RangeReadStreamHeartbeat => {
                Self::RangeReadStreamHeartbeat(body.split_to(body.remaining()))
            }
            MsgType::RangeReadStreamDone => {
                Self::RangeReadStreamDone(body.split_to(body.remaining()))
            }
            MsgType::RangeReadStreamCancel => {
                Self::RangeReadStreamCancel(body.split_to(body.remaining()))
            }
            MsgType::TruncateForward => Self::TruncateForward(body.split_to(body.remaining())),
            MsgType::TruncateAck => Self::TruncateAck(body.split_to(body.remaining())),
            MsgType::RepairMerkleRequest => {
                Self::RepairMerkleRequest(body.split_to(body.remaining()))
            }
            MsgType::RepairMerkleResponse => {
                Self::RepairMerkleResponse(body.split_to(body.remaining()))
            }
            MsgType::RepairFetchRequest => {
                Self::RepairFetchRequest(body.split_to(body.remaining()))
            }
            MsgType::RepairFetchResponse => {
                Self::RepairFetchResponse(body.split_to(body.remaining()))
            }
            MsgType::RepairApplyRequest => {
                Self::RepairApplyRequest(body.split_to(body.remaining()))
            }
            MsgType::RepairApplyResponse => {
                Self::RepairApplyResponse(body.split_to(body.remaining()))
            }
            MsgType::StreamStart => Self::StreamStart(body.split_to(body.remaining())),
            MsgType::StreamChunk => Self::StreamChunk(body.split_to(body.remaining())),
            MsgType::StreamEnd => Self::StreamEnd(body.split_to(body.remaining())),
            MsgType::SstableStreamStart => {
                Self::SstableStreamStart(body.split_to(body.remaining()))
            }
            MsgType::SstableStreamChunk => {
                Self::SstableStreamChunk(body.split_to(body.remaining()))
            }
            MsgType::SstableStreamEnd => Self::SstableStreamEnd(body.split_to(body.remaining())),
            MsgType::PairWriteForward => Self::PairWriteForward(body.split_to(body.remaining())),
            MsgType::PairWriteAck => Self::PairWriteAck(body.split_to(body.remaining())),
            MsgType::PairCatchUpResponse => {
                Self::PairCatchUpResponse(body.split_to(body.remaining()))
            }
            MsgType::PairSchemaSync => Self::PairSchemaSync(body.split_to(body.remaining())),
            MsgType::PairDdlForward => Self::PairDdlForward(body.split_to(body.remaining())),
            MsgType::PairDdlAck => Self::PairDdlAck(body.split_to(body.remaining())),
            MsgType::PairBatchForward => Self::PairBatchForward(body.split_to(body.remaining())),
            MsgType::PairBatchAck => Self::PairBatchAck(body.split_to(body.remaining())),
            MsgType::BatchlogWrite => Self::BatchlogWrite(body.split_to(body.remaining())),
            MsgType::BatchlogDelete => Self::BatchlogDelete(body.split_to(body.remaining())),
            MsgType::BatchlogReplay => Self::BatchlogReplay(body.split_to(body.remaining())),
            MsgType::IndexBuildRequest => Self::IndexBuildRequest(body.split_to(body.remaining())),
            MsgType::IndexBuildComplete => {
                Self::IndexBuildComplete(body.split_to(body.remaining()))
            }
            MsgType::IndexReadRequest => Self::IndexReadRequest(body.split_to(body.remaining())),
            MsgType::IndexReadResponse => Self::IndexReadResponse(body.split_to(body.remaining())),
            MsgType::AccordPreAccept => Self::AccordPreAccept(body.split_to(body.remaining())),
            MsgType::AccordPreAcceptOK => Self::AccordPreAcceptOK(body.split_to(body.remaining())),
            MsgType::AccordAccept => Self::AccordAccept(body.split_to(body.remaining())),
            MsgType::AccordAcceptOK => Self::AccordAcceptOK(body.split_to(body.remaining())),
            MsgType::AccordCommit => Self::AccordCommit(body.split_to(body.remaining())),
            MsgType::AccordRead => Self::AccordRead(body.split_to(body.remaining())),
            MsgType::AccordReadOK => Self::AccordReadOK(body.split_to(body.remaining())),
            MsgType::AccordApply => Self::AccordApply(body.split_to(body.remaining())),
            MsgType::AccordApplyOK => Self::AccordApplyOK(body.split_to(body.remaining())),
            MsgType::AccordRecover => Self::AccordRecover(body.split_to(body.remaining())),
            MsgType::AccordRecoverOK => Self::AccordRecoverOK(body.split_to(body.remaining())),
            MsgType::BootstrapComplete => {
                let mut id_bytes = [0u8; 16];
                if body.remaining() >= 16 {
                    body.copy_to_slice(&mut id_bytes);
                }
                Self::BootstrapComplete {
                    node_id: uuid::Uuid::from_bytes(id_bytes),
                }
            }
            MsgType::BootstrapCompleteAck => Self::BootstrapCompleteAck,
            MsgType::ClusterMembershipForward => {
                Self::ClusterMembershipForward(body.split_to(body.remaining()))
            }
            MsgType::ClusterMembershipForwardAck => {
                Self::ClusterMembershipForwardAck(body.split_to(body.remaining()))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn ping_roundtrip() {
        let msg = Message::Ping {
            nonce: 42,
            sent_at: 1_000_000_000,
        };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::Ping, &mut buf.freeze()).unwrap();
        assert_eq!(
            decoded,
            Message::Ping {
                nonce: 42,
                sent_at: 1_000_000_000
            }
        );
    }

    #[test]
    fn handshake_roundtrip() {
        let msg = Message::Handshake {
            cluster_name: "ferrosa".to_string(),
            host_id: Uuid::new_v4(),
            protocol_version: 1,
            supported_compression: vec![0, 1], // none + lz4
            auth_token: vec![0xAB; 32],
            cql_broadcast: Some("host:19042".into()),
            internode_broadcast: Some("host:17000".into()),
        };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::Handshake, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn handshake_roundtrip_no_broadcast() {
        let msg = Message::Handshake {
            cluster_name: "ferrosa".to_string(),
            host_id: Uuid::new_v4(),
            protocol_version: 1,
            supported_compression: vec![0],
            auth_token: vec![],
            cql_broadcast: None,
            internode_broadcast: None,
        };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::Handshake, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn handshake_ack_roundtrip_with_broadcast() {
        let msg = Message::HandshakeAck {
            host_id: Uuid::new_v4(),
            protocol_version: 1,
            chosen_compression: 0,
            accepted: true,
            reason: String::new(),
            cql_broadcast: Some("192.168.1.5:19042".into()),
            internode_broadcast: Some("192.168.1.5:17000".into()),
        };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::HandshakeAck, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn handshake_backward_compat_no_broadcast_field() {
        // Simulate a pre-extension peer that sends a Handshake without the
        // cql_broadcast trailing field. We manually encode the old format.
        let host_id = Uuid::new_v4();
        let mut buf = BytesMut::new();
        put_string(&mut buf, "ferrosa").unwrap();
        put_uuid(&mut buf, &host_id);
        buf.put_u8(1); // protocol_version
        buf.put_u8(1); // compression list length
        buf.put_u8(0); // supported_compression[0] = none
        put_bytes(&mut buf, &[]).unwrap(); // auth_token (empty)
                                           // No trailing cql_broadcast marker

        let decoded = Message::decode(MsgType::Handshake, &mut buf.freeze()).unwrap();
        match decoded {
            Message::Handshake {
                cql_broadcast,
                internode_broadcast,
                cluster_name,
                host_id: decoded_id,
                ..
            } => {
                assert_eq!(cluster_name, "ferrosa");
                assert_eq!(decoded_id, host_id);
                assert_eq!(cql_broadcast, None);
                assert_eq!(internode_broadcast, None);
            }
            other => panic!("expected Handshake, got {other:?}"),
        }
    }

    #[test]
    fn handshake_backward_compat_cql_present_internode_absent() {
        // A peer that advertises cql_broadcast but predates the
        // internode_broadcast field: the trailing internode marker is missing.
        // Decode must yield the cql_broadcast and a None internode_broadcast.
        let host_id = Uuid::new_v4();
        let mut buf = BytesMut::new();
        put_string(&mut buf, "ferrosa").unwrap();
        put_uuid(&mut buf, &host_id);
        buf.put_u8(1); // protocol_version
        buf.put_u8(1); // compression list length
        buf.put_u8(0); // supported_compression[0] = none
        put_bytes(&mut buf, &[]).unwrap(); // auth_token (empty)
        buf.put_u8(1); // cql_broadcast present
        put_string(&mut buf, "10.0.0.1:19042").unwrap();
        // No trailing internode_broadcast marker.

        let decoded = Message::decode(MsgType::Handshake, &mut buf.freeze()).unwrap();
        match decoded {
            Message::Handshake {
                cql_broadcast,
                internode_broadcast,
                ..
            } => {
                assert_eq!(cql_broadcast.as_deref(), Some("10.0.0.1:19042"));
                assert_eq!(internode_broadcast, None);
            }
            other => panic!("expected Handshake, got {other:?}"),
        }
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
    fn raft_opaque_payload_variants_roundtrip_without_reframing() {
        let variants = vec![
            (
                MsgType::RaftAppendEntries,
                Message::RaftAppendEntries(Bytes::from_static(b"append-entries\0payload")),
            ),
            (
                MsgType::RaftAppendResponse,
                Message::RaftAppendResponse(Bytes::from_static(b"append-response")),
            ),
            (
                MsgType::RaftVote,
                Message::RaftVote(Bytes::from_static(b"vote-request")),
            ),
            (
                MsgType::RaftVoteResponse,
                Message::RaftVoteResponse(Bytes::from_static(b"vote-response")),
            ),
            (
                MsgType::RaftInstallSnapshot,
                Message::RaftInstallSnapshot(Bytes::from_static(b"snapshot-bytes")),
            ),
        ];

        for (msg_type, msg) in variants {
            assert_eq!(msg.msg_type(), msg_type, "msg type mismatch for {msg:?}");
            let mut buf = BytesMut::new();
            msg.encode(&mut buf).unwrap();
            let decoded = Message::decode(msg_type, &mut buf.freeze()).unwrap();
            assert_eq!(decoded, msg, "roundtrip mismatch for {msg_type:?}");
        }
    }

    #[test]
    fn schema_then_query_write_frames_preserve_replay_order_and_payloads() {
        let frames = vec![
            Message::PairSchemaSync(Bytes::from_static(b"schema-snapshot:v1")),
            Message::PairDdlForward(Bytes::from_static(b"create table ks.t")),
            Message::MutationForward(Bytes::from_static(b"insert ks.t pk=1")),
            Message::ReadRequest(Bytes::from_static(b"select ks.t pk=1")),
        ];

        let decoded: Vec<_> = frames
            .iter()
            .map(|frame| {
                let mut buf = BytesMut::new();
                frame.encode(&mut buf).unwrap();
                Message::decode(frame.msg_type(), &mut buf.freeze()).unwrap()
            })
            .collect();

        assert_eq!(decoded, frames);
        assert!(matches!(decoded[0], Message::PairSchemaSync(_)));
        assert!(matches!(decoded[1], Message::PairDdlForward(_)));
        assert!(matches!(decoded[2], Message::MutationForward(_)));
        assert!(matches!(decoded[3], Message::ReadRequest(_)));
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

    #[test]
    fn index_build_request_roundtrip() {
        let payload = Bytes::from(b"test payload".to_vec());
        let msg = Message::IndexBuildRequest(payload.clone());
        assert_eq!(msg.msg_type(), MsgType::IndexBuildRequest);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::IndexBuildRequest, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, Message::IndexBuildRequest(payload));
    }

    #[test]
    fn index_build_complete_roundtrip() {
        let payload = Bytes::from(b"complete".to_vec());
        let msg = Message::IndexBuildComplete(payload.clone());
        assert_eq!(msg.msg_type(), MsgType::IndexBuildComplete);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::IndexBuildComplete, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, Message::IndexBuildComplete(payload));
    }

    #[test]
    fn cluster_membership_forward_roundtrip() {
        let payload = Bytes::from_static(b"serialized-membership-op");
        let msg = Message::ClusterMembershipForward(payload.clone());
        assert_eq!(msg.msg_type(), MsgType::ClusterMembershipForward);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded =
            Message::decode(MsgType::ClusterMembershipForward, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, Message::ClusterMembershipForward(payload));
    }

    #[test]
    fn cluster_membership_forward_ack_roundtrip() {
        let payload = Bytes::from_static(b"forward-ack");
        let msg = Message::ClusterMembershipForwardAck(payload.clone());
        assert_eq!(msg.msg_type(), MsgType::ClusterMembershipForwardAck);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded =
            Message::decode(MsgType::ClusterMembershipForwardAck, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, Message::ClusterMembershipForwardAck(payload));
    }

    #[test]
    fn cluster_membership_forward_msg_types_from_u8() {
        assert_eq!(
            MsgType::try_from(0x82).unwrap(),
            MsgType::ClusterMembershipForward
        );
        assert_eq!(
            MsgType::try_from(0x83).unwrap(),
            MsgType::ClusterMembershipForwardAck
        );
    }

    #[test]
    fn test_cluster_invite_encode_decode_roundtrip() {
        let peers = vec![
            (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap()),
            (Uuid::new_v4(), "10.0.0.2:7000".parse().unwrap()),
            (Uuid::new_v4(), "[::1]:7000".parse().unwrap()),
        ];
        let msg = Message::ClusterInvite {
            initiator: Uuid::new_v4(),
            peers: peers.clone(),
        };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::ClusterInvite, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_cluster_invite_ack_encode_decode_roundtrip() {
        let host_id = Uuid::new_v4();
        let msg = Message::ClusterInviteAck { host_id };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::ClusterInviteAck, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_cluster_invite_empty_peers() {
        let msg = Message::ClusterInvite {
            initiator: Uuid::new_v4(),
            peers: vec![],
        };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        let decoded = Message::decode(MsgType::ClusterInvite, &mut buf.freeze()).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_cluster_invite_msg_type() {
        let msg = Message::ClusterInvite {
            initiator: Uuid::new_v4(),
            peers: vec![],
        };
        assert_eq!(msg.msg_type(), MsgType::ClusterInvite);

        let msg = Message::ClusterInviteAck {
            host_id: Uuid::new_v4(),
        };
        assert_eq!(msg.msg_type(), MsgType::ClusterInviteAck);
    }

    #[test]
    fn cluster_invite_peer_order_permutations_roundtrip_without_losing_membership() {
        let peer_a = (
            Uuid::from_bytes([0xAA, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
            "127.0.0.1:7001".parse().unwrap(),
        );
        let peer_b = (
            Uuid::from_bytes([0xBB, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]),
            "127.0.0.1:7002".parse().unwrap(),
        );
        let peer_c = (
            Uuid::from_bytes([0xCC, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3]),
            "127.0.0.1:7003".parse().unwrap(),
        );
        let permutations = vec![
            vec![peer_a, peer_b, peer_c],
            vec![peer_a, peer_c, peer_b],
            vec![peer_b, peer_a, peer_c],
            vec![peer_b, peer_c, peer_a],
            vec![peer_c, peer_a, peer_b],
            vec![peer_c, peer_b, peer_a],
        ];

        for peers in permutations {
            let msg = Message::ClusterInvite {
                initiator: Uuid::from_bytes([0x11; 16]),
                peers: peers.clone(),
            };
            let mut buf = BytesMut::new();
            msg.encode(&mut buf).unwrap();
            let decoded = Message::decode(MsgType::ClusterInvite, &mut buf.freeze()).unwrap();
            assert_eq!(decoded, msg, "peer order must survive invite roundtrip");
            match decoded {
                Message::ClusterInvite { peers: decoded, .. } => {
                    assert_eq!(decoded.len(), 3);
                    for peer in peers {
                        assert!(decoded.contains(&peer), "missing peer {peer:?}");
                    }
                }
                other => panic!("expected ClusterInvite, got {other:?}"),
            }
        }
    }

    #[test]
    fn cluster_invite_ack_host_ids_roundtrip_for_distinct_join_orders() {
        for first_byte in [0x01, 0x7F, 0xFF] {
            let host_id = Uuid::from_bytes([first_byte; 16]);
            let msg = Message::ClusterInviteAck { host_id };
            let mut buf = BytesMut::new();
            msg.encode(&mut buf).unwrap();
            let decoded = Message::decode(MsgType::ClusterInviteAck, &mut buf.freeze()).unwrap();
            assert_eq!(decoded, msg);
        }
    }

    proptest! {
        #[test]
        fn decode_never_panics(data in proptest::collection::vec(any::<u8>(), 0..512)) {
            let bytes = Bytes::from(data);
            // Try decoding as each message type — should return Ok or Err, never panic
            for msg_type_byte in 0x01..=0x7Au8 {
                if let Ok(msg_type) = MsgType::try_from(msg_type_byte) {
                    let _ = Message::decode(msg_type, &mut bytes.clone());
                }
            }
        }
    }

    /// ADR-020 streaming range-read message variants must encode and
    /// decode losslessly. They are opaque `Bytes` payloads; the
    /// structured shapes (request_id, seq, partitions, …) are bincoded
    /// by ferrosa-cluster, identical to the existing
    /// RangeReadRequest/RangeReadResponse pattern.
    #[test]
    fn streaming_range_read_variants_round_trip_through_encode_decode() {
        let cases: &[(MsgType, Message)] = &[
            (
                MsgType::RangeReadStreamRequest,
                Message::RangeReadStreamRequest(Bytes::from_static(b"req-payload")),
            ),
            (
                MsgType::RangeReadStreamChunk,
                Message::RangeReadStreamChunk(Bytes::from_static(b"chunk-bytes")),
            ),
            (
                MsgType::RangeReadStreamHeartbeat,
                Message::RangeReadStreamHeartbeat(Bytes::from_static(b"hb")),
            ),
            (
                MsgType::RangeReadStreamDone,
                Message::RangeReadStreamDone(Bytes::from_static(b"done")),
            ),
            (
                MsgType::RangeReadStreamCancel,
                Message::RangeReadStreamCancel(Bytes::from_static(b"cancel")),
            ),
        ];

        for (expected_type, msg) in cases {
            assert_eq!(
                msg.msg_type(),
                *expected_type,
                "msg_type() mapping wrong for {msg:?}"
            );
            let mut buf = BytesMut::new();
            msg.encode(&mut buf).expect("encode");
            let decoded = Message::decode(*expected_type, &mut buf.freeze()).expect("decode");
            assert_eq!(decoded, *msg, "round-trip lost data for {msg:?}");
        }
    }
}
