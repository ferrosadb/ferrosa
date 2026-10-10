//! Generated Cap'n Proto protocol modules and adapters for internode messages.
//!
//! Adapter types below provide an explicit domain-facing seam for the Cap'n
//! Proto envelope/body contract so callers do not depend on generated wire
//! structs. The legacy message body remains available as an append-only envelope
//! payload until live peers negotiate the CapnProto frame format.

use std::fmt;

use bytes::{Bytes, BytesMut};
use capnp::{message, serialize};
use uuid::Uuid;

use crate::codec::{MsgType, WireFrameFormat};
use crate::message::Message;
use crate::protocol::envelope_capnp::{
    accord_control, accord_read_predicate, accord_txn_id, accord_write_set_entry,
    bootstrap_control, bootstrap_stream_plan, cluster_control, envelope, error_frame,
    legacy_payload, node_identity, recovery_control, stream_chunk, stream_chunk_metadata,
    stream_control, stream_end, stream_start, ErrorCode, MessageFamily,
};

capnp::generated_code!(pub mod envelope_capnp);

const MAGIC: u32 = 0x4645_5231;
pub const CURRENT_TRANSPORT_VERSION: u16 = 1;
pub const MIN_SUPPORTED_TRANSPORT_VERSION: u16 = 1;
pub const CURRENT_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NodeIdentity {
    pub host_id: Uuid,
    pub node_id: u64,
    pub address: String,
    pub cql_broadcast: Option<String>,
    pub data_center: String,
    pub rack: String,
    pub certificate_fingerprint: Vec<u8>,
}

impl NodeIdentity {
    pub fn minimal(host_id: Uuid, address: impl Into<String>) -> Self {
        Self {
            host_id,
            address: address.into(),
            ..Self::default()
        }
    }

    fn is_unset(&self) -> bool {
        self.host_id.is_nil()
            && self.node_id == 0
            && self.address.is_empty()
            && self.cql_broadcast.is_none()
            && self.data_center.is_empty()
            && self.rack.is_empty()
            && self.certificate_fingerprint.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeLifecycleState {
    Unknown,
    Joining,
    Normal,
    Leaving,
    Left,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryReason {
    Unknown,
    PeerReconnected,
    StaleGeneration,
    LostLocalState,
    RaftLogGap,
    StreamRetry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    NoOp,
    RefreshMetadata,
    ReplayRaft,
    RunBootstrap,
    FullBootstrapRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterControlMessage {
    Invite {
        initiator: NodeIdentity,
        peers: Vec<NodeIdentity>,
        formation_epoch: u64,
        expires_at_unix_nanos: u64,
        invite_id: Uuid,
    },
    InviteAck {
        host: NodeIdentity,
        invite_id: Uuid,
        accepted: bool,
        reason: String,
    },
    RejoinRequest {
        host: NodeIdentity,
        last_known_membership_epoch: u64,
        last_applied_raft_index: u64,
        local_generation: u64,
        wants_bootstrap_plan: bool,
    },
    RejoinPlan {
        membership_epoch: u64,
        state: NodeLifecycleState,
        required_bootstrap: bool,
        bootstrap_plan_id: Option<Uuid>,
        peers: Vec<NodeIdentity>,
        reason: String,
    },
    FormationEpochBump {
        previous: u64,
        next: u64,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryControlMessage {
    Request {
        host: NodeIdentity,
        reason: RecoveryReason,
        last_membership_epoch: u64,
        last_applied_raft_index: u64,
        last_durable_commit_log_segment: u64,
        local_generation: u64,
    },
    Plan {
        plan_id: Uuid,
        membership_epoch: u64,
        action: RecoveryAction,
        bootstrap_plan_id: Option<Uuid>,
        leader: NodeIdentity,
        safe_message: String,
    },
    Progress {
        plan_id: Uuid,
        phase: String,
        completed_units: u64,
        total_units: u64,
    },
    Complete {
        plan_id: Uuid,
        host: NodeIdentity,
        final_membership_epoch: u64,
        final_applied_raft_index: u64,
    },
}

pub const MAX_STREAM_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapStreamPlan {
    SstableBulk { sstable_dir_count: u32 },
    BoundedRows { row_fallback_limit: u32 },
    RetryRequired,
}

impl BootstrapStreamPlan {
    pub fn row_materialization_limit(self) -> Option<u32> {
        match self {
            Self::BoundedRows { row_fallback_limit } => Some(row_fallback_limit),
            Self::SstableBulk { .. } | Self::RetryRequired => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapControlMessage {
    Plan {
        plan_id: Uuid,
        table_id: String,
        plan: BootstrapStreamPlan,
    },
    Progress {
        plan_id: Uuid,
        completed_chunks: u64,
        total_chunks: u64,
        bytes_streamed: u64,
    },
    Complete {
        plan_id: Uuid,
        host: NodeIdentity,
        bytes_streamed: u64,
    },
    Error {
        plan_id: Uuid,
        failed_plan: BootstrapStreamPlan,
        retryable: bool,
        safe_message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    Unknown,
    Sstable,
    RowFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamChunkMetadata {
    pub plan_id: Uuid,
    pub kind: StreamKind,
    pub chunk_index: u64,
    pub byte_offset: u64,
    pub payload_bytes: u32,
    pub crc32c: u32,
    pub is_last: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamControlMessage {
    Start {
        plan_id: Uuid,
        kind: StreamKind,
        total_chunks: u64,
        max_chunk_bytes: u32,
    },
    Chunk {
        metadata: StreamChunkMetadata,
        data: Vec<u8>,
    },
    End {
        plan_id: Uuid,
        kind: StreamKind,
        chunks_sent: u64,
        bytes_sent: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapnpErrorCode {
    MalformedFrame,
    UnsupportedVersion,
    UnsupportedFeature,
    Unauthenticated,
    Unauthorized,
    ClusterMismatch,
    UnknownMessage,
    NoHandler,
    Timeout,
    Overloaded,
    NotLeader,
    StaleEpoch,
    Conflict,
    RetryRequired,
    FullBootstrapRequired,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapnpErrorFrame {
    pub code: CapnpErrorCode,
    pub retryable: bool,
    pub safe_message: String,
    pub detail_code: String,
    pub failed_family: MessageFamily,
    pub failed_kind: u16,
    pub failed_correlation_id: Option<Uuid>,
    pub min_supported_transport_version: u16,
    pub max_supported_transport_version: u16,
    pub missing_features: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyPayload {
    pub msg_type: u16,
    pub body: Vec<u8>,
}

/// Accord's total-order execution stamp (`ferrosa_common::accord::Timestamp`),
/// mirrored here so the wire contract does not depend on the cluster crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct AccordTxnId {
    pub epoch: u64,
    pub time: u64,
    pub seq: u32,
    pub node: u64,
}

/// One write in a multi-key Accord transaction's write-set: the partition key
/// (Accord conflict ordering + replica routing) paired with the encoded
/// commit-log mutation to apply for that key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccordWriteSetEntry {
    pub key: Vec<u8>,
    pub mutation: Vec<u8>,
}

/// The read a linearizable read-vote performs, mirroring
/// `ferrosa-cluster`'s `ReadPredicate` wire tags exactly.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum AccordReadPredicate {
    /// `INSERT IF NOT EXISTS` existence semantics; also the fail-closed default.
    #[default]
    NotExists,
    /// Generic `IF`: read the whole partition's row at `t`.
    ReadRow { keyspace: String, table: String },
    /// PostgreSQL snapshot-barrier read-vote.
    SnapshotBarrier,
    /// Unconditional commit: no `IF`, always applies after commit.
    Always,
    /// A conditional statement on one row (partition + clustering).
    ReadClusteringRow {
        keyspace: String,
        table: String,
        clustering: Vec<u8>,
    },
}

/// The Accord consensus family payloads, mirroring the bincode shapes in
/// `ferrosa-cluster`'s `accord::wire` module 1:1.
///
/// These are the wire contract for [`MessageFamily::Accord`] frames once a peer
/// has negotiated [`CapnpPayload::Accord`] support; until then Accord messages
/// ride [`LegacyPayload`]. The [`Self::ApplyV2`] variant is the bulk data path —
/// a transactional `COPY` rides its whole write-set in one of these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccordControlMessage {
    /// Multi-key Apply request (`wire.rs` `ApplyV2Payload`).
    ApplyV2 {
        txn_id: AccordTxnId,
        writes: Vec<AccordWriteSetEntry>,
    },
    /// The region-REFERENCE multi-key Apply header. The write-set payload bytes do
    /// NOT travel here: they follow this message as one contiguous REGION, addressed
    /// by `offsets[i]`/`lengths[i]` relative to the region's start. The generic
    /// envelope round-trip carries the index only; the region is spliced on / sliced
    /// off by [`encode_accord_apply_v2_region`] and
    /// [`decode_accord_apply_v2_region`].
    ApplyV2Region {
        txn_id: AccordTxnId,
        /// Byte offset of entry `i` within the appended region.
        offsets: Vec<u64>,
        /// Byte length of entry `i`.
        lengths: Vec<u32>,
        /// Region codec tag (see [`RegionCodec::tag`]); 0 = uncompressed.
        compression: u8,
        /// Uncompressed region length.
        uncompressed_len: u64,
        /// Block size the region was compressed in (0 = one block).
        block_bytes: u32,
    },
    /// Single-key Apply request (`wire.rs` `ApplyPayload`), the degenerate one-key case.
    Apply {
        txn_id: AccordTxnId,
        result_data: Vec<u8>,
    },
    /// Apply acknowledgement (`wire.rs` `ApplyOkPayload`).
    ApplyOk { txn_id: AccordTxnId, from: u64 },
    /// Single-key PreAccept probe (`wire.rs` `PreAcceptPayload`).
    PreAccept {
        txn_id: AccordTxnId,
        t0: AccordTxnId,
        key: Vec<u8>,
        ballot: u64,
        epoch: u64,
    },
    /// Multi-key PreAccept probe carrying every conflict key (`wire.rs` `PreAcceptV2Payload`).
    PreAcceptV2 {
        txn_id: AccordTxnId,
        t0: AccordTxnId,
        keys: Vec<Vec<u8>>,
        ballot: u64,
        epoch: u64,
        snapshot_ts: Option<AccordTxnId>,
    },
    /// PreAccept reply (`wire.rs` `PreAcceptOkPayload`).
    PreAcceptOk {
        from: u64,
        t: AccordTxnId,
        deps: Vec<AccordTxnId>,
        snapshot_stale: bool,
    },
    /// Accept request, slow path (`wire.rs` `AcceptPayload`).
    Accept {
        txn_id: AccordTxnId,
        t0: AccordTxnId,
        t: AccordTxnId,
        deps: Vec<AccordTxnId>,
        ballot: u64,
    },
    /// Accept reply (`wire.rs` `AcceptOkPayload` — a pre-dependency coordinator's
    /// shorter body decodes here as an empty `deps`).
    AcceptOk {
        txn_id: AccordTxnId,
        deps: Vec<AccordTxnId>,
    },
    /// Commit broadcast (`wire.rs` `CommitPayload`).
    Commit {
        txn_id: AccordTxnId,
        t0: AccordTxnId,
        t: AccordTxnId,
        deps: Vec<AccordTxnId>,
    },
    /// Commit acknowledgement (`wire.rs` `CommitOkPayload`).
    CommitOk { txn_id: AccordTxnId, from: u64 },
    /// Recovery probe (`wire.rs` `RecoverPayload`).
    Recover {
        txn_id: AccordTxnId,
        t0: AccordTxnId,
        ballot: u64,
    },
    /// Recovery acknowledgement — carries no payload (the bincode path ships an
    /// empty body for `AccordRecoverOK`).
    RecoverOk,
    /// Linearizable read-vote request (`wire.rs` `ReadVotePayload`).
    Read {
        txn_id: AccordTxnId,
        t: AccordTxnId,
        key: Vec<u8>,
        predicate: AccordReadPredicate,
    },
    /// Read-vote response (`wire.rs` `ReadVoteOkPayload`).
    ReadOk {
        txn_id: AccordTxnId,
        from: u64,
        condition_holds: bool,
        current_row: Vec<u8>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapnpTransportMode {
    LegacyOnly,
    PreferCapnp,
    RequireCapnp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapnpCapabilityNegotiation {
    pub frame_format: WireFrameFormat,
    pub enabled_features: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedMessageEnvelope {
    pub stream_id: u64,
    pub correlation_id: Uuid,
    pub message: Message,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapnpPayload {
    Cluster(ClusterControlMessage),
    Recovery(RecoveryControlMessage),
    Bootstrap(BootstrapControlMessage),
    Stream(StreamControlMessage),
    Accord(AccordControlMessage),
    Error(CapnpErrorFrame),
    Legacy(LegacyPayload),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapnpEnvelope {
    pub transport_version: u16,
    pub min_supported_transport_version: u16,
    pub schema_version: u16,
    pub required_features: u64,
    pub optional_features: u64,
    pub sender: NodeIdentity,
    pub recipient: Option<NodeIdentity>,
    pub cluster_id: Uuid,
    pub epoch: u64,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
    pub stream_id: u64,
    pub sequence: u64,
    pub deadline_unix_nanos: Option<u64>,
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub trace_flags: u64,
    pub payload: CapnpPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapnpDecodeError {
    MalformedFrame(String),
    UnsupportedVersion {
        transport_version: u16,
        min_supported_transport_version: u16,
    },
    UnsupportedFeature {
        missing_features: u64,
    },
    InvalidRequiredField(String),
    UnknownPayload(String),
}

impl fmt::Display for CapnpDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedFrame(e) => write!(f, "malformed CapnProto frame: {e}"),
            Self::UnsupportedVersion {
                transport_version,
                min_supported_transport_version,
            } => write!(
                f,
                "unsupported CapnProto transport version {transport_version} (min {min_supported_transport_version})"
            ),
            Self::UnsupportedFeature { missing_features } => write!(
                f,
                "unsupported CapnProto required features: 0x{missing_features:016x}"
            ),
            Self::InvalidRequiredField(field) => write!(f, "invalid required field: {field}"),
            Self::UnknownPayload(payload) => write!(f, "unknown CapnProto payload: {payload}"),
        }
    }
}

impl std::error::Error for CapnpDecodeError {}

impl From<capnp::Error> for CapnpDecodeError {
    fn from(value: capnp::Error) -> Self {
        Self::MalformedFrame(value.to_string())
    }
}

impl From<std::str::Utf8Error> for CapnpDecodeError {
    fn from(value: std::str::Utf8Error) -> Self {
        Self::MalformedFrame(format!("invalid UTF-8 text field: {value}"))
    }
}

/// Stamp the common envelope header fields (everything except the payload
/// union) from `envelope` onto a freshly-initialized root builder.
///
/// Shared by the generic [`encode_envelope`] path and the direct-from-borrow
/// Accord Apply encoder [`encode_accord_apply_v2`], so both stamp transport
/// version, feature bits and trace fields identically and a peer sees one
/// envelope shape.
fn stamp_envelope_header(root: &mut envelope::Builder<'_>, envelope: &CapnpEnvelope) {
    root.set_magic(MAGIC);
    root.set_transport_version(envelope.transport_version);
    root.set_min_supported_transport_version(envelope.min_supported_transport_version);
    root.set_schema_version(envelope.schema_version);
    root.set_flags(0);
    root.set_required_features(envelope.required_features);
    root.set_optional_features(envelope.optional_features);
    fill_node(root.reborrow().init_sender(), &envelope.sender);
    if let Some(recipient) = &envelope.recipient {
        fill_node(root.reborrow().init_recipient(), recipient);
    }
    root.set_cluster_id(envelope.cluster_id.as_bytes());
    root.set_epoch(envelope.epoch);
    root.set_correlation_id(envelope.correlation_id.as_bytes());
    if let Some(causation_id) = envelope.causation_id {
        root.set_causation_id(causation_id.as_bytes());
    }
    root.set_stream_id(envelope.stream_id);
    root.set_sequence(envelope.sequence);
    root.set_deadline_unix_nanos(envelope.deadline_unix_nanos.unwrap_or(0));
    root.set_trace_id(&envelope.trace_id);
    root.set_span_id(&envelope.span_id);
    root.set_trace_flags(envelope.trace_flags);
}

pub fn encode_envelope(envelope: &CapnpEnvelope) -> Result<Vec<u8>, CapnpDecodeError> {
    validate_envelope(envelope)?;

    let mut message = message::Builder::new_default();
    {
        let mut root = message.init_root::<envelope::Builder>();
        stamp_envelope_header(&mut root, envelope);

        match &envelope.payload {
            CapnpPayload::Cluster(cluster) => {
                root.set_message_family(MessageFamily::ClusterControl);
                write_cluster(cluster, root.init_payload().init_cluster())?;
            }
            CapnpPayload::Recovery(recovery) => {
                root.set_message_family(MessageFamily::Recovery);
                write_recovery(recovery, root.init_payload().init_recovery())?;
            }
            CapnpPayload::Bootstrap(bootstrap) => {
                root.set_message_family(MessageFamily::Bootstrap);
                write_bootstrap(bootstrap, root.init_payload().init_bootstrap())?;
            }
            CapnpPayload::Stream(stream) => {
                root.set_message_family(MessageFamily::Stream);
                write_stream(stream, root.init_payload().init_stream())?;
            }
            CapnpPayload::Accord(accord) => {
                root.set_message_family(MessageFamily::Accord);
                root.set_message_kind(accord_message_kind(accord));
                write_accord(accord, root.init_payload().init_accord())?;
            }
            CapnpPayload::Error(error) => {
                root.set_message_family(error.failed_family);
                root.set_message_kind(error.failed_kind);
                write_error(error, root.init_payload().init_error())?;
            }
            CapnpPayload::Legacy(legacy) => {
                root.set_message_family(message_family_for_kind(legacy.msg_type));
                root.set_message_kind(legacy.msg_type);
                write_legacy(legacy, root.init_payload().init_legacy())?;
            }
        }
    }

    Ok(serialize::write_message_to_words(&message))
}

pub fn decode_envelope(bytes: &[u8]) -> Result<CapnpEnvelope, CapnpDecodeError> {
    let reader = serialize::read_message(
        &mut std::io::Cursor::new(bytes),
        message::ReaderOptions {
            traversal_limit_in_words: Some(8 * 1024 * 1024),
            nesting_limit: 64,
        },
    )?;
    let root = reader.get_root::<envelope::Reader>()?;

    if root.get_magic() != MAGIC {
        return Err(CapnpDecodeError::MalformedFrame(format!(
            "bad magic: 0x{:08x}",
            root.get_magic()
        )));
    }
    let transport_version = root.get_transport_version();
    let min_supported_transport_version = root.get_min_supported_transport_version();
    if transport_version > CURRENT_TRANSPORT_VERSION
        && min_supported_transport_version > CURRENT_TRANSPORT_VERSION
    {
        return Err(CapnpDecodeError::UnsupportedVersion {
            transport_version,
            min_supported_transport_version,
        });
    }

    let payload = match root.get_payload().which().map_err(not_in_schema)? {
        envelope::payload::Cluster(cluster) => {
            CapnpPayload::Cluster(read_cluster(cluster.map_err(CapnpDecodeError::from)?)?)
        }
        envelope::payload::Recovery(recovery) => {
            CapnpPayload::Recovery(read_recovery(recovery.map_err(CapnpDecodeError::from)?)?)
        }
        envelope::payload::Bootstrap(bootstrap) => {
            CapnpPayload::Bootstrap(read_bootstrap(bootstrap.map_err(CapnpDecodeError::from)?)?)
        }
        envelope::payload::Stream(stream) => {
            CapnpPayload::Stream(read_stream(stream.map_err(CapnpDecodeError::from)?)?)
        }
        envelope::payload::Accord(accord) => {
            CapnpPayload::Accord(read_accord(accord.map_err(CapnpDecodeError::from)?)?)
        }
        envelope::payload::Error(error) => {
            CapnpPayload::Error(read_error(error.map_err(CapnpDecodeError::from)?)?)
        }
        envelope::payload::Legacy(legacy) => {
            CapnpPayload::Legacy(read_legacy(legacy.map_err(CapnpDecodeError::from)?)?)
        }
        _ => {
            return Err(CapnpDecodeError::UnknownPayload(
                "non-adapter payload".to_string(),
            ))
        }
    };

    Ok(CapnpEnvelope {
        transport_version,
        min_supported_transport_version,
        schema_version: root.get_schema_version(),
        required_features: root.get_required_features(),
        optional_features: root.get_optional_features(),
        sender: read_node(root.get_sender()?)?,
        recipient: read_optional_node(root.get_recipient()?)?,
        cluster_id: read_optional_uuid(root.get_cluster_id()?)?.unwrap_or_default(),
        epoch: root.get_epoch(),
        correlation_id: read_optional_uuid(root.get_correlation_id()?)?.unwrap_or_default(),
        causation_id: read_optional_uuid(root.get_causation_id()?)?,
        stream_id: root.get_stream_id(),
        sequence: root.get_sequence(),
        deadline_unix_nanos: nonzero(root.get_deadline_unix_nanos()),
        trace_id: data_array::<16>(root.get_trace_id()?)?.unwrap_or([0; 16]),
        span_id: data_array::<8>(root.get_span_id()?)?.unwrap_or([0; 8]),
        trace_flags: root.get_trace_flags(),
        payload,
    })
}

pub fn negotiate_capnp_transport(
    mode: CapnpTransportMode,
    peer_min_supported_transport_version: u16,
    peer_transport_version: u16,
) -> Result<WireFrameFormat, CapnpDecodeError> {
    let peer_supports_current = peer_min_supported_transport_version <= CURRENT_TRANSPORT_VERSION
        && peer_transport_version >= CURRENT_TRANSPORT_VERSION;
    match mode {
        CapnpTransportMode::LegacyOnly => Ok(WireFrameFormat::Legacy),
        CapnpTransportMode::PreferCapnp if peer_supports_current => {
            Ok(WireFrameFormat::CapnpEnvelope)
        }
        CapnpTransportMode::PreferCapnp => Ok(WireFrameFormat::Legacy),
        CapnpTransportMode::RequireCapnp if peer_supports_current => {
            Ok(WireFrameFormat::CapnpEnvelope)
        }
        CapnpTransportMode::RequireCapnp => Err(CapnpDecodeError::UnsupportedVersion {
            transport_version: peer_transport_version,
            min_supported_transport_version: peer_min_supported_transport_version,
        }),
    }
}

pub fn negotiate_capnp_capabilities(
    mode: CapnpTransportMode,
    peer_min_supported_transport_version: u16,
    peer_transport_version: u16,
    local_supported_features: u64,
    peer_required_features: u64,
) -> Result<CapnpCapabilityNegotiation, CapnpDecodeError> {
    let frame_format = negotiate_capnp_transport(
        mode,
        peer_min_supported_transport_version,
        peer_transport_version,
    )?;
    if frame_format == WireFrameFormat::Legacy {
        return Ok(CapnpCapabilityNegotiation {
            frame_format,
            enabled_features: 0,
        });
    }

    let missing_features = peer_required_features & !local_supported_features;
    if missing_features != 0 {
        return Err(CapnpDecodeError::UnsupportedFeature { missing_features });
    }

    Ok(CapnpCapabilityNegotiation {
        frame_format,
        enabled_features: peer_required_features & local_supported_features,
    })
}

pub fn validate_capnp_capabilities(
    envelope: &CapnpEnvelope,
    local_supported_features: u64,
) -> Result<(), CapnpDecodeError> {
    let missing_features = envelope.required_features & !local_supported_features;
    if missing_features == 0 {
        Ok(())
    } else {
        Err(CapnpDecodeError::UnsupportedFeature { missing_features })
    }
}

pub fn encode_message_envelope(
    message: &Message,
    stream_id: u64,
    correlation_id: Uuid,
) -> Result<Vec<u8>, CapnpDecodeError> {
    let mut body = BytesMut::new();
    message.encode(&mut body).map_err(|err| {
        CapnpDecodeError::MalformedFrame(format!("legacy message encode failed: {err}"))
    })?;
    let envelope = base_envelope(
        correlation_id,
        stream_id,
        CapnpPayload::Legacy(LegacyPayload {
            msg_type: message.msg_type() as u16,
            body: body.to_vec(),
        }),
    );
    encode_envelope(&envelope)
}

pub fn decode_message_envelope(bytes: &[u8]) -> Result<DecodedMessageEnvelope, CapnpDecodeError> {
    let envelope = decode_envelope(bytes)?;
    let CapnpPayload::Legacy(payload) = envelope.payload else {
        return Err(CapnpDecodeError::UnknownPayload(
            "expected legacy message payload".to_string(),
        ));
    };
    let msg_type = MsgType::try_from(u8::try_from(payload.msg_type).map_err(|_| {
        CapnpDecodeError::UnknownPayload(format!("message kind out of range: {}", payload.msg_type))
    })?)
    .map_err(|err| CapnpDecodeError::UnknownPayload(err.to_string()))?;
    let message = Message::decode(msg_type, &mut Bytes::from(payload.body)).map_err(|err| {
        CapnpDecodeError::MalformedFrame(format!("legacy message body decode failed: {err}"))
    })?;
    Ok(DecodedMessageEnvelope {
        stream_id: envelope.stream_id,
        correlation_id: envelope.correlation_id,
        message,
    })
}

pub fn encode_error_envelope(
    code: CapnpErrorCode,
    retryable: bool,
    safe_message: &str,
    failed_msg_type: MsgType,
    failed_correlation_id: Uuid,
    min_supported_transport_version: u16,
    max_supported_transport_version: u16,
) -> Result<Vec<u8>, CapnpDecodeError> {
    let error = CapnpErrorFrame {
        code,
        retryable,
        safe_message: safe_message.to_string(),
        detail_code: String::new(),
        failed_family: message_family_for_kind(failed_msg_type as u16),
        failed_kind: failed_msg_type as u16,
        failed_correlation_id: Some(failed_correlation_id),
        min_supported_transport_version,
        max_supported_transport_version,
        missing_features: 0,
    };
    encode_envelope(&base_envelope(
        failed_correlation_id,
        0,
        CapnpPayload::Error(error),
    ))
}

fn base_envelope(correlation_id: Uuid, stream_id: u64, payload: CapnpPayload) -> CapnpEnvelope {
    CapnpEnvelope {
        transport_version: CURRENT_TRANSPORT_VERSION,
        min_supported_transport_version: MIN_SUPPORTED_TRANSPORT_VERSION,
        schema_version: CURRENT_SCHEMA_VERSION,
        required_features: 0,
        optional_features: 0,
        sender: NodeIdentity::minimal(Uuid::from_bytes([1; 16]), "0.0.0.0:0"),
        recipient: None,
        cluster_id: Uuid::nil(),
        epoch: 0,
        correlation_id,
        causation_id: None,
        stream_id,
        sequence: 0,
        deadline_unix_nanos: None,
        trace_id: [0; 16],
        span_id: [0; 8],
        trace_flags: 0,
        payload,
    }
}

fn validate_envelope(envelope: &CapnpEnvelope) -> Result<(), CapnpDecodeError> {
    match &envelope.payload {
        CapnpPayload::Cluster(_) | CapnpPayload::Recovery(_) | CapnpPayload::Bootstrap(_) => {
            validate_node("sender", &envelope.sender)?;
        }
        CapnpPayload::Stream(_)
        | CapnpPayload::Accord(_)
        | CapnpPayload::Error(_)
        | CapnpPayload::Legacy(_) => {}
    }
    match &envelope.payload {
        CapnpPayload::Cluster(msg) => validate_cluster(msg),
        CapnpPayload::Recovery(msg) => validate_recovery(msg),
        CapnpPayload::Bootstrap(msg) => validate_bootstrap(msg),
        CapnpPayload::Stream(msg) => validate_stream(msg),
        CapnpPayload::Accord(msg) => validate_accord(msg),
        CapnpPayload::Error(_) | CapnpPayload::Legacy(_) => Ok(()),
    }
}

/// Validate an Accord payload's shape.
///
/// An Accord body carries no sender identity of its own, so — like Stream — it
/// needs no node check. The one field that must not be silently empty is a
/// read-vote's target table: a row read with an empty keyspace/table is a
/// routing bug that would make F+1 agreement meaningless, so it fails loud here
/// rather than reading the wrong (or no) partition.
fn validate_accord(msg: &AccordControlMessage) -> Result<(), CapnpDecodeError> {
    if let AccordControlMessage::Read { predicate, .. } = msg {
        match predicate {
            AccordReadPredicate::ReadRow { keyspace, table } => {
                if keyspace.is_empty() || table.is_empty() {
                    return Err(CapnpDecodeError::InvalidRequiredField(
                        "accord read row keyspace/table".to_string(),
                    ));
                }
            }
            AccordReadPredicate::ReadClusteringRow {
                keyspace, table, ..
            } => {
                if keyspace.is_empty() || table.is_empty() {
                    return Err(CapnpDecodeError::InvalidRequiredField(
                        "accord read clustering row keyspace/table".to_string(),
                    ));
                }
            }
            AccordReadPredicate::NotExists
            | AccordReadPredicate::SnapshotBarrier
            | AccordReadPredicate::Always => {}
        }
    }
    Ok(())
}

fn validate_cluster(msg: &ClusterControlMessage) -> Result<(), CapnpDecodeError> {
    match msg {
        ClusterControlMessage::Invite {
            initiator,
            peers,
            invite_id,
            ..
        } => {
            validate_node("initiator", initiator)?;
            validate_uuid("invite_id", *invite_id)?;
            for (idx, peer) in peers.iter().enumerate() {
                validate_node(&format!("peers[{idx}]"), peer)?;
            }
        }
        ClusterControlMessage::InviteAck {
            host, invite_id, ..
        } => {
            validate_node("host", host)?;
            validate_uuid("invite_id", *invite_id)?;
        }
        ClusterControlMessage::RejoinRequest { host, .. } => validate_node("host", host)?,
        ClusterControlMessage::RejoinPlan { peers, .. } => {
            for (idx, peer) in peers.iter().enumerate() {
                validate_node(&format!("peers[{idx}]"), peer)?;
            }
        }
        ClusterControlMessage::FormationEpochBump { .. } => {}
    }
    Ok(())
}

fn validate_recovery(msg: &RecoveryControlMessage) -> Result<(), CapnpDecodeError> {
    match msg {
        RecoveryControlMessage::Request { host, .. } => validate_node("host", host)?,
        RecoveryControlMessage::Plan {
            plan_id,
            bootstrap_plan_id,
            leader,
            ..
        } => {
            validate_uuid("plan_id", *plan_id)?;
            if let Some(id) = bootstrap_plan_id {
                validate_uuid("bootstrap_plan_id", *id)?;
            }
            validate_node("leader", leader)?;
        }
        RecoveryControlMessage::Progress { plan_id, .. }
        | RecoveryControlMessage::Complete { plan_id, .. } => validate_uuid("plan_id", *plan_id)?,
    }
    if let RecoveryControlMessage::Complete { host, .. } = msg {
        validate_node("host", host)?;
    }
    Ok(())
}

fn validate_bootstrap(msg: &BootstrapControlMessage) -> Result<(), CapnpDecodeError> {
    match msg {
        BootstrapControlMessage::Plan {
            plan_id, table_id, ..
        } => {
            validate_uuid("plan_id", *plan_id)?;
            if table_id.is_empty() {
                return Err(CapnpDecodeError::InvalidRequiredField(
                    "table_id".to_string(),
                ));
            }
        }
        BootstrapControlMessage::Progress { plan_id, .. } => validate_uuid("plan_id", *plan_id)?,
        BootstrapControlMessage::Complete { plan_id, host, .. } => {
            validate_uuid("plan_id", *plan_id)?;
            validate_node("host", host)?;
        }
        BootstrapControlMessage::Error {
            plan_id,
            safe_message,
            ..
        } => {
            validate_uuid("plan_id", *plan_id)?;
            if safe_message.is_empty() {
                return Err(CapnpDecodeError::InvalidRequiredField(
                    "safe_message".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_stream(msg: &StreamControlMessage) -> Result<(), CapnpDecodeError> {
    match msg {
        StreamControlMessage::Start {
            plan_id,
            max_chunk_bytes,
            ..
        } => {
            validate_uuid("plan_id", *plan_id)?;
            if (*max_chunk_bytes as usize) > MAX_STREAM_CHUNK_BYTES {
                return Err(CapnpDecodeError::InvalidRequiredField(format!(
                    "stream chunk max {max_chunk_bytes} exceeds {MAX_STREAM_CHUNK_BYTES}"
                )));
            }
        }
        StreamControlMessage::Chunk { metadata, data } => {
            validate_uuid("metadata.plan_id", metadata.plan_id)?;
            if metadata.payload_bytes as usize != data.len() {
                return Err(CapnpDecodeError::InvalidRequiredField(format!(
                    "payload_bytes {} does not match data length {}",
                    metadata.payload_bytes,
                    data.len()
                )));
            }
            if data.len() > MAX_STREAM_CHUNK_BYTES {
                return Err(CapnpDecodeError::InvalidRequiredField(format!(
                    "stream chunk {} exceeds {MAX_STREAM_CHUNK_BYTES}",
                    data.len()
                )));
            }
        }
        StreamControlMessage::End { plan_id, .. } => validate_uuid("plan_id", *plan_id)?,
    }
    Ok(())
}

fn validate_node(prefix: &str, node: &NodeIdentity) -> Result<(), CapnpDecodeError> {
    if node.host_id.is_nil() {
        return Err(CapnpDecodeError::InvalidRequiredField(format!(
            "{prefix}.host_id"
        )));
    }
    if node.address.is_empty() {
        return Err(CapnpDecodeError::InvalidRequiredField(format!(
            "{prefix}.address"
        )));
    }
    Ok(())
}

fn validate_uuid(field: &str, id: Uuid) -> Result<(), CapnpDecodeError> {
    if id.is_nil() {
        Err(CapnpDecodeError::InvalidRequiredField(field.to_string()))
    } else {
        Ok(())
    }
}

fn fill_node(mut builder: node_identity::Builder<'_>, node: &NodeIdentity) {
    builder.set_host_id(node.host_id.as_bytes());
    builder.set_node_id(node.node_id);
    builder.set_address(&node.address);
    builder.set_cql_broadcast(node.cql_broadcast.as_deref().unwrap_or(""));
    builder.set_data_center(&node.data_center);
    builder.set_rack(&node.rack);
    builder.set_certificate_fingerprint(&node.certificate_fingerprint);
}

fn read_node(reader: node_identity::Reader<'_>) -> Result<NodeIdentity, CapnpDecodeError> {
    Ok(NodeIdentity {
        host_id: read_required_uuid("node.host_id", reader.get_host_id()?)?,
        node_id: reader.get_node_id(),
        address: reader.get_address()?.to_string()?,
        cql_broadcast: text_optional(reader.get_cql_broadcast()?)?,
        data_center: reader.get_data_center()?.to_string()?,
        rack: reader.get_rack()?.to_string()?,
        certificate_fingerprint: reader.get_certificate_fingerprint()?.to_vec(),
    })
}

fn read_optional_node(
    reader: node_identity::Reader<'_>,
) -> Result<Option<NodeIdentity>, CapnpDecodeError> {
    let node = read_node_permissive(reader)?;
    Ok(if node.is_unset() { None } else { Some(node) })
}

fn read_node_permissive(
    reader: node_identity::Reader<'_>,
) -> Result<NodeIdentity, CapnpDecodeError> {
    Ok(NodeIdentity {
        host_id: read_optional_uuid(reader.get_host_id()?)?.unwrap_or_default(),
        node_id: reader.get_node_id(),
        address: reader.get_address()?.to_string()?,
        cql_broadcast: text_optional(reader.get_cql_broadcast()?)?,
        data_center: reader.get_data_center()?.to_string()?,
        rack: reader.get_rack()?.to_string()?,
        certificate_fingerprint: reader.get_certificate_fingerprint()?.to_vec(),
    })
}

fn write_cluster(
    msg: &ClusterControlMessage,
    builder: cluster_control::Builder<'_>,
) -> Result<(), CapnpDecodeError> {
    match msg {
        ClusterControlMessage::Invite {
            initiator,
            peers,
            formation_epoch,
            expires_at_unix_nanos,
            invite_id,
        } => {
            let mut invite = builder.init_op().init_invite();
            fill_node(invite.reborrow().init_initiator(), initiator);
            fill_nodes(invite.reborrow().init_peers(peers.len() as u32), peers);
            invite.set_formation_epoch(*formation_epoch);
            invite.set_expires_at_unix_nanos(*expires_at_unix_nanos);
            invite.set_invite_id(invite_id.as_bytes());
        }
        ClusterControlMessage::InviteAck {
            host,
            invite_id,
            accepted,
            reason,
        } => {
            let mut ack = builder.init_op().init_invite_ack();
            fill_node(ack.reborrow().init_host(), host);
            ack.set_invite_id(invite_id.as_bytes());
            ack.set_accepted(*accepted);
            ack.set_reason(reason);
        }
        ClusterControlMessage::RejoinRequest {
            host,
            last_known_membership_epoch,
            last_applied_raft_index,
            local_generation,
            wants_bootstrap_plan,
        } => {
            let mut request = builder.init_op().init_rejoin_request();
            fill_node(request.reborrow().init_host(), host);
            request.set_last_known_membership_epoch(*last_known_membership_epoch);
            request.set_last_applied_raft_index(*last_applied_raft_index);
            request.set_local_generation(*local_generation);
            request.set_wants_bootstrap_plan(*wants_bootstrap_plan);
        }
        ClusterControlMessage::RejoinPlan {
            membership_epoch,
            state,
            required_bootstrap,
            bootstrap_plan_id,
            peers,
            reason,
        } => {
            let mut plan = builder.init_op().init_rejoin_plan();
            plan.set_membership_epoch(*membership_epoch);
            plan.set_state((*state).into());
            plan.set_required_bootstrap(*required_bootstrap);
            if let Some(id) = bootstrap_plan_id {
                plan.set_bootstrap_plan_id(id.as_bytes());
            }
            fill_nodes(plan.reborrow().init_peers(peers.len() as u32), peers);
            plan.set_reason(reason);
        }
        ClusterControlMessage::FormationEpochBump {
            previous,
            next,
            reason,
        } => {
            let mut bump = builder.init_op().init_formation_epoch_bump();
            bump.set_previous(*previous);
            bump.set_next(*next);
            bump.set_reason(reason);
        }
    }
    Ok(())
}

fn write_recovery(
    msg: &RecoveryControlMessage,
    builder: recovery_control::Builder<'_>,
) -> Result<(), CapnpDecodeError> {
    match msg {
        RecoveryControlMessage::Request {
            host,
            reason,
            last_membership_epoch,
            last_applied_raft_index,
            last_durable_commit_log_segment,
            local_generation,
        } => {
            let mut request = builder.init_op().init_request();
            fill_node(request.reborrow().init_host(), host);
            request.set_reason((*reason).into());
            request.set_last_membership_epoch(*last_membership_epoch);
            request.set_last_applied_raft_index(*last_applied_raft_index);
            request.set_last_durable_commit_log_segment(*last_durable_commit_log_segment);
            request.set_local_generation(*local_generation);
        }
        RecoveryControlMessage::Plan {
            plan_id,
            membership_epoch,
            action,
            bootstrap_plan_id,
            leader,
            safe_message,
        } => {
            let mut plan = builder.init_op().init_plan();
            plan.set_plan_id(plan_id.as_bytes());
            plan.set_membership_epoch(*membership_epoch);
            plan.set_action((*action).into());
            if let Some(id) = bootstrap_plan_id {
                plan.set_bootstrap_plan_id(id.as_bytes());
            }
            fill_node(plan.reborrow().init_leader(), leader);
            plan.set_safe_message(safe_message);
        }
        RecoveryControlMessage::Progress {
            plan_id,
            phase,
            completed_units,
            total_units,
        } => {
            let mut progress = builder.init_op().init_progress();
            progress.set_plan_id(plan_id.as_bytes());
            progress.set_phase(phase);
            progress.set_completed_units(*completed_units);
            progress.set_total_units(*total_units);
        }
        RecoveryControlMessage::Complete {
            plan_id,
            host,
            final_membership_epoch,
            final_applied_raft_index,
        } => {
            let mut complete = builder.init_op().init_complete();
            complete.set_plan_id(plan_id.as_bytes());
            fill_node(complete.reborrow().init_host(), host);
            complete.set_final_membership_epoch(*final_membership_epoch);
            complete.set_final_applied_raft_index(*final_applied_raft_index);
        }
    }
    Ok(())
}

fn fill_nodes(
    mut builder: capnp::struct_list::Builder<'_, node_identity::Owned>,
    nodes: &[NodeIdentity],
) {
    for (idx, node) in nodes.iter().enumerate() {
        fill_node(builder.reborrow().get(idx as u32), node);
    }
}

fn read_cluster(
    reader: cluster_control::Reader<'_>,
) -> Result<ClusterControlMessage, CapnpDecodeError> {
    Ok(match reader.get_op().which().map_err(not_in_schema)? {
        cluster_control::op::Invite(invite) => {
            let invite = invite?;
            ClusterControlMessage::Invite {
                initiator: read_node(invite.get_initiator()?)?,
                peers: read_nodes(invite.get_peers()?)?,
                formation_epoch: invite.get_formation_epoch(),
                expires_at_unix_nanos: invite.get_expires_at_unix_nanos(),
                invite_id: read_required_uuid("invite_id", invite.get_invite_id()?)?,
            }
        }
        cluster_control::op::InviteAck(ack) => {
            let ack = ack?;
            ClusterControlMessage::InviteAck {
                host: read_node(ack.get_host()?)?,
                invite_id: read_required_uuid("invite_id", ack.get_invite_id()?)?,
                accepted: ack.get_accepted(),
                reason: ack.get_reason()?.to_string()?,
            }
        }
        cluster_control::op::RejoinRequest(request) => {
            let request = request?;
            ClusterControlMessage::RejoinRequest {
                host: read_node(request.get_host()?)?,
                last_known_membership_epoch: request.get_last_known_membership_epoch(),
                last_applied_raft_index: request.get_last_applied_raft_index(),
                local_generation: request.get_local_generation(),
                wants_bootstrap_plan: request.get_wants_bootstrap_plan(),
            }
        }
        cluster_control::op::RejoinPlan(plan) => {
            let plan = plan?;
            ClusterControlMessage::RejoinPlan {
                membership_epoch: plan.get_membership_epoch(),
                state: plan
                    .get_state()
                    .map(NodeLifecycleState::from)
                    .unwrap_or(NodeLifecycleState::Unknown),
                required_bootstrap: plan.get_required_bootstrap(),
                bootstrap_plan_id: read_optional_uuid(plan.get_bootstrap_plan_id()?)?,
                peers: read_nodes(plan.get_peers()?)?,
                reason: plan.get_reason()?.to_string()?,
            }
        }
        cluster_control::op::FormationEpochBump(bump) => {
            let bump = bump?;
            ClusterControlMessage::FormationEpochBump {
                previous: bump.get_previous(),
                next: bump.get_next(),
                reason: bump.get_reason()?.to_string()?,
            }
        }
    })
}

fn read_recovery(
    reader: recovery_control::Reader<'_>,
) -> Result<RecoveryControlMessage, CapnpDecodeError> {
    Ok(match reader.get_op().which().map_err(not_in_schema)? {
        recovery_control::op::Request(request) => {
            let request = request?;
            RecoveryControlMessage::Request {
                host: read_node(request.get_host()?)?,
                reason: request
                    .get_reason()
                    .map(RecoveryReason::from)
                    .unwrap_or(RecoveryReason::Unknown),
                last_membership_epoch: request.get_last_membership_epoch(),
                last_applied_raft_index: request.get_last_applied_raft_index(),
                last_durable_commit_log_segment: request.get_last_durable_commit_log_segment(),
                local_generation: request.get_local_generation(),
            }
        }
        recovery_control::op::Plan(plan) => {
            let plan = plan?;
            RecoveryControlMessage::Plan {
                plan_id: read_required_uuid("plan_id", plan.get_plan_id()?)?,
                membership_epoch: plan.get_membership_epoch(),
                action: plan
                    .get_action()
                    .map(RecoveryAction::from)
                    .unwrap_or(RecoveryAction::NoOp),
                bootstrap_plan_id: read_optional_uuid(plan.get_bootstrap_plan_id()?)?,
                leader: read_node(plan.get_leader()?)?,
                safe_message: plan.get_safe_message()?.to_string()?,
            }
        }
        recovery_control::op::Progress(progress) => {
            let progress = progress?;
            RecoveryControlMessage::Progress {
                plan_id: read_required_uuid("plan_id", progress.get_plan_id()?)?,
                phase: progress.get_phase()?.to_string()?,
                completed_units: progress.get_completed_units(),
                total_units: progress.get_total_units(),
            }
        }
        recovery_control::op::Complete(complete) => {
            let complete = complete?;
            RecoveryControlMessage::Complete {
                plan_id: read_required_uuid("plan_id", complete.get_plan_id()?)?,
                host: read_node(complete.get_host()?)?,
                final_membership_epoch: complete.get_final_membership_epoch(),
                final_applied_raft_index: complete.get_final_applied_raft_index(),
            }
        }
    })
}

fn write_bootstrap(
    msg: &BootstrapControlMessage,
    builder: bootstrap_control::Builder<'_>,
) -> Result<(), CapnpDecodeError> {
    match msg {
        BootstrapControlMessage::Plan {
            plan_id,
            table_id,
            plan,
        } => {
            let mut out = builder.init_op().init_plan();
            out.set_plan_id(plan_id.as_bytes());
            out.set_table_id(table_id);
            write_bootstrap_stream_plan(plan, out.reborrow().init_stream_plan());
        }
        BootstrapControlMessage::Progress {
            plan_id,
            completed_chunks,
            total_chunks,
            bytes_streamed,
        } => {
            let mut out = builder.init_op().init_progress();
            out.set_plan_id(plan_id.as_bytes());
            out.set_completed_chunks(*completed_chunks);
            out.set_total_chunks(*total_chunks);
            out.set_bytes_streamed(*bytes_streamed);
        }
        BootstrapControlMessage::Complete {
            plan_id,
            host,
            bytes_streamed,
        } => {
            let mut out = builder.init_op().init_complete();
            out.set_plan_id(plan_id.as_bytes());
            fill_node(out.reborrow().init_host(), host);
            out.set_bytes_streamed(*bytes_streamed);
        }
        BootstrapControlMessage::Error {
            plan_id,
            failed_plan,
            retryable,
            safe_message,
        } => {
            let mut out = builder.init_op().init_error();
            out.set_plan_id(plan_id.as_bytes());
            write_bootstrap_stream_plan(failed_plan, out.reborrow().init_failed_plan());
            out.set_retryable(*retryable);
            out.set_safe_message(safe_message);
        }
    }
    Ok(())
}

fn write_bootstrap_stream_plan(
    plan: &BootstrapStreamPlan,
    builder: envelope_capnp::bootstrap_stream_plan::Builder<'_>,
) {
    match plan {
        BootstrapStreamPlan::SstableBulk { sstable_dir_count } => builder
            .init_mode()
            .init_sstable_bulk()
            .set_sstable_dir_count(*sstable_dir_count),
        BootstrapStreamPlan::BoundedRows { row_fallback_limit } => builder
            .init_mode()
            .init_bounded_rows()
            .set_row_fallback_limit(*row_fallback_limit),
        BootstrapStreamPlan::RetryRequired => {
            builder.init_mode().init_retry_required();
        }
    }
}

fn read_bootstrap(
    reader: bootstrap_control::Reader<'_>,
) -> Result<BootstrapControlMessage, CapnpDecodeError> {
    Ok(match reader.get_op().which().map_err(not_in_schema)? {
        bootstrap_control::op::Plan(plan) => {
            let plan = plan?;
            BootstrapControlMessage::Plan {
                plan_id: read_required_uuid("plan_id", plan.get_plan_id()?)?,
                table_id: plan.get_table_id()?.to_string()?,
                plan: read_bootstrap_stream_plan(plan.get_stream_plan()?)?,
            }
        }
        bootstrap_control::op::Progress(progress) => {
            let progress = progress?;
            BootstrapControlMessage::Progress {
                plan_id: read_required_uuid("plan_id", progress.get_plan_id()?)?,
                completed_chunks: progress.get_completed_chunks(),
                total_chunks: progress.get_total_chunks(),
                bytes_streamed: progress.get_bytes_streamed(),
            }
        }
        bootstrap_control::op::Complete(complete) => {
            let complete = complete?;
            BootstrapControlMessage::Complete {
                plan_id: read_required_uuid("plan_id", complete.get_plan_id()?)?,
                host: read_node(complete.get_host()?)?,
                bytes_streamed: complete.get_bytes_streamed(),
            }
        }
        bootstrap_control::op::Error(error) => {
            let error = error?;
            BootstrapControlMessage::Error {
                plan_id: read_required_uuid("plan_id", error.get_plan_id()?)?,
                failed_plan: read_bootstrap_stream_plan(error.get_failed_plan()?)?,
                retryable: error.get_retryable(),
                safe_message: error.get_safe_message()?.to_string()?,
            }
        }
    })
}

fn read_bootstrap_stream_plan(
    reader: envelope_capnp::bootstrap_stream_plan::Reader<'_>,
) -> Result<BootstrapStreamPlan, CapnpDecodeError> {
    Ok(match reader.get_mode().which().map_err(not_in_schema)? {
        bootstrap_stream_plan::mode::SstableBulk(plan) => BootstrapStreamPlan::SstableBulk {
            sstable_dir_count: plan?.get_sstable_dir_count(),
        },
        bootstrap_stream_plan::mode::BoundedRows(plan) => BootstrapStreamPlan::BoundedRows {
            row_fallback_limit: plan?.get_row_fallback_limit(),
        },
        bootstrap_stream_plan::mode::RetryRequired(_) => BootstrapStreamPlan::RetryRequired,
    })
}

fn write_stream(
    msg: &StreamControlMessage,
    builder: stream_control::Builder<'_>,
) -> Result<(), CapnpDecodeError> {
    match msg {
        StreamControlMessage::Start {
            plan_id,
            kind,
            total_chunks,
            max_chunk_bytes,
        } => {
            let mut out = builder.init_op().init_start();
            write_stream_start(
                plan_id,
                *kind,
                *total_chunks,
                *max_chunk_bytes,
                out.reborrow(),
            );
        }
        StreamControlMessage::Chunk { metadata, data } => {
            let mut out = builder.init_op().init_chunk();
            write_stream_chunk(metadata, data, out.reborrow());
        }
        StreamControlMessage::End {
            plan_id,
            kind,
            chunks_sent,
            bytes_sent,
        } => {
            let mut out = builder.init_op().init_end();
            write_stream_end(plan_id, *kind, *chunks_sent, *bytes_sent, out.reborrow());
        }
    }
    Ok(())
}

fn write_stream_start(
    plan_id: &Uuid,
    kind: StreamKind,
    total_chunks: u64,
    max_chunk_bytes: u32,
    mut builder: stream_start::Builder<'_>,
) {
    builder.set_plan_id(plan_id.as_bytes());
    builder.set_kind(kind.into());
    builder.set_total_chunks(total_chunks);
    builder.set_max_chunk_bytes(max_chunk_bytes);
}

fn write_stream_chunk(
    metadata: &StreamChunkMetadata,
    data: &[u8],
    mut builder: stream_chunk::Builder<'_>,
) {
    write_stream_chunk_metadata(metadata, builder.reborrow().init_metadata());
    builder.set_data(data);
}

fn write_stream_chunk_metadata(
    metadata: &StreamChunkMetadata,
    mut builder: stream_chunk_metadata::Builder<'_>,
) {
    builder.set_plan_id(metadata.plan_id.as_bytes());
    builder.set_kind(metadata.kind.into());
    builder.set_chunk_index(metadata.chunk_index);
    builder.set_byte_offset(metadata.byte_offset);
    builder.set_payload_bytes(metadata.payload_bytes);
    builder.set_crc32c(metadata.crc32c);
    builder.set_is_last(metadata.is_last);
}

fn write_stream_end(
    plan_id: &Uuid,
    kind: StreamKind,
    chunks_sent: u64,
    bytes_sent: u64,
    mut builder: stream_end::Builder<'_>,
) {
    builder.set_plan_id(plan_id.as_bytes());
    builder.set_kind(kind.into());
    builder.set_chunks_sent(chunks_sent);
    builder.set_bytes_sent(bytes_sent);
}

fn read_stream(
    reader: stream_control::Reader<'_>,
) -> Result<StreamControlMessage, CapnpDecodeError> {
    Ok(match reader.get_op().which().map_err(not_in_schema)? {
        stream_control::op::Start(start) => {
            let start = start?;
            StreamControlMessage::Start {
                plan_id: read_required_uuid("plan_id", start.get_plan_id()?)?,
                kind: start
                    .get_kind()
                    .map(StreamKind::from)
                    .unwrap_or(StreamKind::Unknown),
                total_chunks: start.get_total_chunks(),
                max_chunk_bytes: start.get_max_chunk_bytes(),
            }
        }
        stream_control::op::Chunk(chunk) => {
            let chunk = chunk?;
            let data = chunk.get_data()?.to_vec();
            if data.len() > MAX_STREAM_CHUNK_BYTES {
                return Err(CapnpDecodeError::InvalidRequiredField(format!(
                    "stream chunk {} exceeds {MAX_STREAM_CHUNK_BYTES}",
                    data.len()
                )));
            }
            let metadata = read_stream_chunk_metadata(chunk.get_metadata()?)?;
            if metadata.payload_bytes as usize != data.len() {
                return Err(CapnpDecodeError::InvalidRequiredField(format!(
                    "payload_bytes {} does not match data length {}",
                    metadata.payload_bytes,
                    data.len()
                )));
            }
            StreamControlMessage::Chunk { metadata, data }
        }
        stream_control::op::End(end) => {
            let end = end?;
            StreamControlMessage::End {
                plan_id: read_required_uuid("plan_id", end.get_plan_id()?)?,
                kind: end
                    .get_kind()
                    .map(StreamKind::from)
                    .unwrap_or(StreamKind::Unknown),
                chunks_sent: end.get_chunks_sent(),
                bytes_sent: end.get_bytes_sent(),
            }
        }
    })
}

fn read_stream_chunk_metadata(
    reader: stream_chunk_metadata::Reader<'_>,
) -> Result<StreamChunkMetadata, CapnpDecodeError> {
    Ok(StreamChunkMetadata {
        plan_id: read_required_uuid("metadata.plan_id", reader.get_plan_id()?)?,
        kind: reader
            .get_kind()
            .map(StreamKind::from)
            .unwrap_or(StreamKind::Unknown),
        chunk_index: reader.get_chunk_index(),
        byte_offset: reader.get_byte_offset(),
        payload_bytes: reader.get_payload_bytes(),
        crc32c: reader.get_crc32c(),
        is_last: reader.get_is_last(),
    })
}

fn read_nodes(
    reader: capnp::struct_list::Reader<'_, node_identity::Owned>,
) -> Result<Vec<NodeIdentity>, CapnpDecodeError> {
    (0..reader.len())
        .map(|idx| read_node(reader.get(idx)))
        .collect()
}

fn write_error(
    msg: &CapnpErrorFrame,
    mut builder: error_frame::Builder<'_>,
) -> Result<(), CapnpDecodeError> {
    builder.set_code(msg.code.into());
    builder.set_retryable(msg.retryable);
    builder.set_safe_message(&msg.safe_message);
    builder.set_detail_code(&msg.detail_code);
    builder.set_failed_family(msg.failed_family);
    builder.set_failed_kind(msg.failed_kind);
    if let Some(id) = msg.failed_correlation_id {
        builder.set_failed_correlation_id(id.as_bytes());
    }
    builder.set_min_supported_transport_version(msg.min_supported_transport_version);
    builder.set_max_supported_transport_version(msg.max_supported_transport_version);
    builder.set_missing_features(msg.missing_features);
    Ok(())
}

fn read_error(reader: error_frame::Reader<'_>) -> Result<CapnpErrorFrame, CapnpDecodeError> {
    Ok(CapnpErrorFrame {
        code: reader.get_code().map_err(not_in_schema)?.into(),
        retryable: reader.get_retryable(),
        safe_message: reader.get_safe_message()?.to_string()?,
        detail_code: reader.get_detail_code()?.to_string()?,
        failed_family: reader.get_failed_family().map_err(not_in_schema)?,
        failed_kind: reader.get_failed_kind(),
        failed_correlation_id: read_optional_uuid(reader.get_failed_correlation_id()?)?,
        min_supported_transport_version: reader.get_min_supported_transport_version(),
        max_supported_transport_version: reader.get_max_supported_transport_version(),
        missing_features: reader.get_missing_features(),
    })
}

fn write_legacy(
    payload: &LegacyPayload,
    mut builder: legacy_payload::Builder<'_>,
) -> Result<(), CapnpDecodeError> {
    builder.set_msg_type(payload.msg_type);
    builder.set_body(&payload.body);
    Ok(())
}

fn read_legacy(reader: legacy_payload::Reader<'_>) -> Result<LegacyPayload, CapnpDecodeError> {
    Ok(LegacyPayload {
        msg_type: reader.get_msg_type(),
        body: reader.get_body()?.to_vec(),
    })
}

// ---------------------------------------------------------------------------
// Accord family adapters (ADR-019).
//
// These mirror `ferrosa-cluster`'s `accord::wire` bincode payloads 1:1 so that a
// capnp-encoded Accord frame decodes to EXACTLY what the bincode frame decoded
// to. The Apply family is the bulk data path; the rest carry Accord's
// identity/dependency bookkeeping.
// ---------------------------------------------------------------------------

/// The bincode `MsgType` discriminant an Accord payload rides under, so a capnp
/// Accord frame's `messageKind` matches what the bincode frame carried.
fn accord_message_kind(msg: &AccordControlMessage) -> u16 {
    let msg_type = match msg {
        AccordControlMessage::ApplyV2 { .. } => MsgType::AccordApplyV2,
        AccordControlMessage::ApplyV2Region { .. } => MsgType::AccordApplyV2Region,
        AccordControlMessage::Apply { .. } => MsgType::AccordApply,
        AccordControlMessage::ApplyOk { .. } => MsgType::AccordApplyOK,
        AccordControlMessage::PreAccept { .. } => MsgType::AccordPreAccept,
        AccordControlMessage::PreAcceptV2 { .. } => MsgType::AccordPreAcceptV2,
        AccordControlMessage::PreAcceptOk { .. } => MsgType::AccordPreAcceptOK,
        AccordControlMessage::Accept { .. } => MsgType::AccordAccept,
        AccordControlMessage::AcceptOk { .. } => MsgType::AccordAcceptOK,
        AccordControlMessage::Commit { .. } => MsgType::AccordCommit,
        // There is no `AccordCommitOK` wire code: the commit ack reuses the
        // request's discriminant, exactly as it does on the bincode path.
        AccordControlMessage::CommitOk { .. } => MsgType::AccordCommit,
        AccordControlMessage::Recover { .. } => MsgType::AccordRecover,
        AccordControlMessage::RecoverOk => MsgType::AccordRecoverOK,
        AccordControlMessage::Read { .. } => MsgType::AccordRead,
        AccordControlMessage::ReadOk { .. } => MsgType::AccordReadOK,
    };
    msg_type as u16
}

fn write_accord_txn_id(txn_id: AccordTxnId, mut builder: accord_txn_id::Builder<'_>) {
    builder.set_epoch(txn_id.epoch);
    builder.set_time(txn_id.time);
    builder.set_seq(txn_id.seq);
    builder.set_node(txn_id.node);
}

fn read_accord_txn_id(reader: accord_txn_id::Reader<'_>) -> AccordTxnId {
    AccordTxnId {
        epoch: reader.get_epoch(),
        time: reader.get_time(),
        seq: reader.get_seq(),
        node: reader.get_node(),
    }
}

fn read_accord_txn_ids(
    reader: capnp::struct_list::Reader<'_, accord_txn_id::Owned>,
) -> Result<Vec<AccordTxnId>, CapnpDecodeError> {
    (0..reader.len())
        .map(|idx| Ok(read_accord_txn_id(reader.get(idx))))
        .collect()
}

fn write_accord_txn_ids(
    deps: &[AccordTxnId],
    builder: capnp::struct_list::Builder<'_, accord_txn_id::Owned>,
) {
    let mut builder = builder;
    for (idx, dep) in deps.iter().enumerate() {
        write_accord_txn_id(*dep, builder.reborrow().get(idx as u32));
    }
}

fn write_accord_writes(
    writes: &[AccordWriteSetEntry],
    builder: capnp::struct_list::Builder<'_, accord_write_set_entry::Owned>,
) {
    let mut builder = builder;
    for (idx, write) in writes.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_key(&write.key);
        entry.set_mutation(&write.mutation);
    }
}

fn read_accord_writes(
    reader: capnp::struct_list::Reader<'_, accord_write_set_entry::Owned>,
) -> Result<Vec<AccordWriteSetEntry>, CapnpDecodeError> {
    (0..reader.len())
        .map(|idx| {
            let entry = reader.get(idx);
            Ok(AccordWriteSetEntry {
                key: entry.get_key()?.to_vec(),
                mutation: entry.get_mutation()?.to_vec(),
            })
        })
        .collect()
}

fn write_accord_read_predicate(
    predicate: &AccordReadPredicate,
    builder: accord_read_predicate::Builder<'_>,
) {
    let mut op = builder.init_op();
    match predicate {
        AccordReadPredicate::NotExists => op.set_not_exists(()),
        AccordReadPredicate::ReadRow { keyspace, table } => {
            let mut row = op.init_read_row();
            row.set_keyspace(keyspace);
            row.set_table(table);
        }
        AccordReadPredicate::SnapshotBarrier => op.set_snapshot_barrier(()),
        AccordReadPredicate::Always => op.set_always(()),
        AccordReadPredicate::ReadClusteringRow {
            keyspace,
            table,
            clustering,
        } => {
            let mut row = op.init_read_clustering_row();
            row.set_keyspace(keyspace);
            row.set_table(table);
            row.set_clustering(clustering);
        }
    }
}

fn read_accord_read_predicate(
    reader: accord_read_predicate::Reader<'_>,
) -> Result<AccordReadPredicate, CapnpDecodeError> {
    use accord_read_predicate::op;
    Ok(match reader.get_op().which().map_err(not_in_schema)? {
        op::NotExists(()) => AccordReadPredicate::NotExists,
        op::ReadRow(row) => {
            let row = row?;
            AccordReadPredicate::ReadRow {
                keyspace: row.get_keyspace()?.to_string()?,
                table: row.get_table()?.to_string()?,
            }
        }
        op::SnapshotBarrier(()) => AccordReadPredicate::SnapshotBarrier,
        op::Always(()) => AccordReadPredicate::Always,
        op::ReadClusteringRow(row) => {
            let row = row?;
            AccordReadPredicate::ReadClusteringRow {
                keyspace: row.get_keyspace()?.to_string()?,
                table: row.get_table()?.to_string()?,
                clustering: row.get_clustering()?.to_vec(),
            }
        }
    })
}

fn write_accord(
    msg: &AccordControlMessage,
    builder: accord_control::Builder<'_>,
) -> Result<(), CapnpDecodeError> {
    match msg {
        AccordControlMessage::ApplyV2 { txn_id, writes } => {
            let mut out = builder.init_op().init_apply_v2();
            write_accord_txn_id(*txn_id, out.reborrow().init_txn_id());
            write_accord_writes(writes, out.init_writes(writes.len() as u32));
        }
        AccordControlMessage::ApplyV2Region {
            txn_id,
            offsets,
            lengths,
            compression,
            uncompressed_len,
            block_bytes,
        } => {
            let mut out = builder.init_op().init_apply_v2_region();
            write_accord_txn_id(*txn_id, out.reborrow().init_txn_id());
            out.set_entry_count(offsets.len() as u32);
            {
                let mut list = out.reborrow().init_offsets(offsets.len() as u32);
                for (idx, offset) in offsets.iter().enumerate() {
                    list.set(idx as u32, *offset);
                }
            }
            {
                let mut list = out.reborrow().init_lengths(lengths.len() as u32);
                for (idx, len) in lengths.iter().enumerate() {
                    list.set(idx as u32, *len);
                }
            }
            out.set_compression(*compression);
            out.set_uncompressed_len(*uncompressed_len);
            out.set_block_bytes(*block_bytes);
        }
        AccordControlMessage::Apply {
            txn_id,
            result_data,
        } => {
            let mut out = builder.init_op().init_apply();
            write_accord_txn_id(*txn_id, out.reborrow().init_txn_id());
            out.set_result_data(result_data);
        }
        AccordControlMessage::ApplyOk { txn_id, from } => {
            let mut out = builder.init_op().init_apply_ok();
            write_accord_txn_id(*txn_id, out.reborrow().init_txn_id());
            out.set_from(*from);
        }
        AccordControlMessage::PreAccept {
            txn_id,
            t0,
            key,
            ballot,
            epoch,
        } => {
            let mut out = builder.init_op().init_pre_accept();
            write_accord_txn_id(*txn_id, out.reborrow().init_txn_id());
            write_accord_txn_id(*t0, out.reborrow().init_t0());
            out.set_key(key);
            out.set_ballot(*ballot);
            out.set_epoch(*epoch);
        }
        AccordControlMessage::PreAcceptV2 {
            txn_id,
            t0,
            keys,
            ballot,
            epoch,
            snapshot_ts,
        } => {
            let mut out = builder.init_op().init_pre_accept_v2();
            write_accord_txn_id(*txn_id, out.reborrow().init_txn_id());
            write_accord_txn_id(*t0, out.reborrow().init_t0());
            {
                let mut list = out.reborrow().init_keys(keys.len() as u32);
                for (idx, key) in keys.iter().enumerate() {
                    list.set(idx as u32, key);
                }
            }
            out.set_ballot(*ballot);
            out.set_epoch(*epoch);
            if let Some(snapshot_ts) = snapshot_ts {
                write_accord_txn_id(*snapshot_ts, out.init_snapshot_ts());
            }
        }
        AccordControlMessage::PreAcceptOk {
            from,
            t,
            deps,
            snapshot_stale,
        } => {
            let mut out = builder.init_op().init_pre_accept_ok();
            out.set_from(*from);
            write_accord_txn_id(*t, out.reborrow().init_t());
            write_accord_txn_ids(deps, out.reborrow().init_deps(deps.len() as u32));
            out.set_snapshot_stale(*snapshot_stale);
        }
        AccordControlMessage::Accept {
            txn_id,
            t0,
            t,
            deps,
            ballot,
        } => {
            let mut out = builder.init_op().init_accept();
            write_accord_txn_id(*txn_id, out.reborrow().init_txn_id());
            write_accord_txn_id(*t0, out.reborrow().init_t0());
            write_accord_txn_id(*t, out.reborrow().init_t());
            write_accord_txn_ids(deps, out.reborrow().init_deps(deps.len() as u32));
            out.set_ballot(*ballot);
        }
        AccordControlMessage::AcceptOk { txn_id, deps } => {
            let mut out = builder.init_op().init_accept_ok();
            write_accord_txn_id(*txn_id, out.reborrow().init_txn_id());
            write_accord_txn_ids(deps, out.init_deps(deps.len() as u32));
        }
        AccordControlMessage::Commit {
            txn_id,
            t0,
            t,
            deps,
        } => {
            let mut out = builder.init_op().init_commit();
            write_accord_txn_id(*txn_id, out.reborrow().init_txn_id());
            write_accord_txn_id(*t0, out.reborrow().init_t0());
            write_accord_txn_id(*t, out.reborrow().init_t());
            write_accord_txn_ids(deps, out.init_deps(deps.len() as u32));
        }
        AccordControlMessage::CommitOk { txn_id, from } => {
            let mut out = builder.init_op().init_commit_ok();
            write_accord_txn_id(*txn_id, out.reborrow().init_txn_id());
            out.set_from(*from);
        }
        AccordControlMessage::Recover { txn_id, t0, ballot } => {
            let mut out = builder.init_op().init_recover();
            write_accord_txn_id(*txn_id, out.reborrow().init_txn_id());
            write_accord_txn_id(*t0, out.reborrow().init_t0());
            out.set_ballot(*ballot);
        }
        AccordControlMessage::RecoverOk => {
            builder.init_op().set_recover_ok(());
        }
        AccordControlMessage::Read {
            txn_id,
            t,
            key,
            predicate,
        } => {
            let mut out = builder.init_op().init_read();
            write_accord_txn_id(*txn_id, out.reborrow().init_txn_id());
            write_accord_txn_id(*t, out.reborrow().init_t());
            out.set_key(key);
            write_accord_read_predicate(predicate, out.init_predicate());
        }
        AccordControlMessage::ReadOk {
            txn_id,
            from,
            condition_holds,
            current_row,
        } => {
            let mut out = builder.init_op().init_read_ok();
            write_accord_txn_id(*txn_id, out.reborrow().init_txn_id());
            out.set_from(*from);
            out.set_condition_holds(*condition_holds);
            out.set_current_row(current_row);
        }
    }
    Ok(())
}

fn read_accord(
    reader: accord_control::Reader<'_>,
) -> Result<AccordControlMessage, CapnpDecodeError> {
    use accord_control::op;
    Ok(match reader.get_op().which().map_err(not_in_schema)? {
        op::ApplyV2(apply) => {
            let apply = apply?;
            AccordControlMessage::ApplyV2 {
                txn_id: read_accord_txn_id(apply.get_txn_id()?),
                writes: read_accord_writes(apply.get_writes()?)?,
            }
        }
        op::ApplyV2Region(region) => {
            let region = region?;
            let offsets = region.get_offsets()?.iter().collect();
            let lengths = region.get_lengths()?.iter().collect();
            AccordControlMessage::ApplyV2Region {
                txn_id: read_accord_txn_id(region.get_txn_id()?),
                offsets,
                lengths,
                compression: region.get_compression(),
                uncompressed_len: region.get_uncompressed_len(),
                block_bytes: region.get_block_bytes(),
            }
        }
        op::Apply(apply) => {
            let apply = apply?;
            AccordControlMessage::Apply {
                txn_id: read_accord_txn_id(apply.get_txn_id()?),
                result_data: apply.get_result_data()?.to_vec(),
            }
        }
        op::ApplyOk(ok) => {
            let ok = ok?;
            AccordControlMessage::ApplyOk {
                txn_id: read_accord_txn_id(ok.get_txn_id()?),
                from: ok.get_from(),
            }
        }
        op::PreAccept(v) => {
            let v = v?;
            AccordControlMessage::PreAccept {
                txn_id: read_accord_txn_id(v.get_txn_id()?),
                t0: read_accord_txn_id(v.get_t0()?),
                key: v.get_key()?.to_vec(),
                ballot: v.get_ballot(),
                epoch: v.get_epoch(),
            }
        }
        op::PreAcceptV2(v) => {
            let v = v?;
            let keys = v.get_keys()?;
            let keys = (0..keys.len())
                .map(|idx| keys.get(idx).map(<[u8]>::to_vec))
                .collect::<Result<Vec<_>, _>>()?;
            AccordControlMessage::PreAcceptV2 {
                txn_id: read_accord_txn_id(v.get_txn_id()?),
                t0: read_accord_txn_id(v.get_t0()?),
                keys,
                ballot: v.get_ballot(),
                epoch: v.get_epoch(),
                snapshot_ts: if v.has_snapshot_ts() {
                    Some(read_accord_txn_id(v.get_snapshot_ts()?))
                } else {
                    None
                },
            }
        }
        op::PreAcceptOk(ok) => {
            let ok = ok?;
            AccordControlMessage::PreAcceptOk {
                from: ok.get_from(),
                t: read_accord_txn_id(ok.get_t()?),
                deps: read_accord_txn_ids(ok.get_deps()?)?,
                snapshot_stale: ok.get_snapshot_stale(),
            }
        }
        op::Accept(v) => {
            let v = v?;
            AccordControlMessage::Accept {
                txn_id: read_accord_txn_id(v.get_txn_id()?),
                t0: read_accord_txn_id(v.get_t0()?),
                t: read_accord_txn_id(v.get_t()?),
                deps: read_accord_txn_ids(v.get_deps()?)?,
                ballot: v.get_ballot(),
            }
        }
        op::AcceptOk(ok) => {
            let ok = ok?;
            AccordControlMessage::AcceptOk {
                txn_id: read_accord_txn_id(ok.get_txn_id()?),
                deps: read_accord_txn_ids(ok.get_deps()?)?,
            }
        }
        op::Commit(v) => {
            let v = v?;
            AccordControlMessage::Commit {
                txn_id: read_accord_txn_id(v.get_txn_id()?),
                t0: read_accord_txn_id(v.get_t0()?),
                t: read_accord_txn_id(v.get_t()?),
                deps: read_accord_txn_ids(v.get_deps()?)?,
            }
        }
        op::CommitOk(ok) => {
            let ok = ok?;
            AccordControlMessage::CommitOk {
                txn_id: read_accord_txn_id(ok.get_txn_id()?),
                from: ok.get_from(),
            }
        }
        op::Recover(v) => {
            let v = v?;
            AccordControlMessage::Recover {
                txn_id: read_accord_txn_id(v.get_txn_id()?),
                t0: read_accord_txn_id(v.get_t0()?),
                ballot: v.get_ballot(),
            }
        }
        op::RecoverOk(()) => AccordControlMessage::RecoverOk,
        op::Read(v) => {
            let v = v?;
            AccordControlMessage::Read {
                txn_id: read_accord_txn_id(v.get_txn_id()?),
                t: read_accord_txn_id(v.get_t()?),
                key: v.get_key()?.to_vec(),
                predicate: read_accord_read_predicate(v.get_predicate()?)?,
            }
        }
        op::ReadOk(ok) => {
            let ok = ok?;
            AccordControlMessage::ReadOk {
                txn_id: read_accord_txn_id(ok.get_txn_id()?),
                from: ok.get_from(),
                condition_holds: ok.get_condition_holds(),
                current_row: ok.get_current_row()?.to_vec(),
            }
        }
    })
}

/// Encode one Accord payload into a standalone capnp envelope.
///
/// The transport-version and feature fields are stamped exactly as the generic
/// [`encode_envelope`] path stamps them, so a frame produced here is the frame a
/// peer that negotiated [`CapnpPayload::Accord`] receives.
pub fn encode_accord_envelope(payload: &AccordControlMessage) -> Result<Vec<u8>, CapnpDecodeError> {
    encode_envelope(&base_envelope(
        Uuid::nil(),
        0,
        CapnpPayload::Accord(payload.clone()),
    ))
}

/// Decode a frame produced by [`encode_accord_envelope`] back to its Accord payload.
pub fn decode_accord_envelope(bytes: &[u8]) -> Result<AccordControlMessage, CapnpDecodeError> {
    match decode_envelope(bytes)?.payload {
        CapnpPayload::Accord(payload) => Ok(payload),
        other => Err(CapnpDecodeError::UnknownPayload(format!(
            "expected an Accord payload, found {other:?}"
        ))),
    }
}

/// Encode a multi-key Accord Apply (`AccordControlMessage::ApplyV2`) directly from
/// borrowed `(key, mutation)` byte slices, writing each entry straight into the
/// capnp arena as it is visited.
///
/// This is the hot-path encoder for a transactional bulk load (`pgbench -i`'s
/// `TRUNCATE + COPY + COMMIT`, ~1M rows in ONE transaction). The coordinator walks
/// its write-set once and copies each entry's bytes once INTO the message. There is
/// no intermediate owned `Vec<AccordWriteSetEntry>` and no clone of the write-set —
/// which is exactly the extra alloc-per-entry plus full-write-set clone that a
/// `encode_accord_envelope(&ApplyV2Payload::to_capnp())` call would pay on top of the
/// copy the wire itself needs.
///
/// `writes` is an `ExactSizeIterator` so the capnp list is sized once
/// (`init_writes(len)`) and never reallocated.
pub fn encode_accord_apply_v2<'a, I>(
    txn_id: AccordTxnId,
    writes: I,
) -> Result<Vec<u8>, CapnpDecodeError>
where
    I: ExactSizeIterator<Item = (&'a [u8], &'a [u8])>,
{
    // Build the header from the same shape `encode_accord_envelope` stamps, so a
    // peer cannot tell the two encoders apart except by the payload bytes.
    let base = base_envelope(
        Uuid::nil(),
        0,
        CapnpPayload::Accord(AccordControlMessage::ApplyV2 {
            txn_id,
            writes: Vec::new(),
        }),
    );
    let mut message = message::Builder::new_default();
    {
        let mut root = message.init_root::<envelope::Builder>();
        stamp_envelope_header(&mut root, &base);
        root.set_message_family(MessageFamily::Accord);
        root.set_message_kind(MsgType::AccordApplyV2 as u16);
        let mut apply = root.init_payload().init_accord().init_op().init_apply_v2();
        write_accord_txn_id(txn_id, apply.reborrow().init_txn_id());
        let mut list = apply.init_writes(writes.len() as u32);
        for (idx, (key, mutation)) in writes.enumerate() {
            let mut entry = list.reborrow().get(idx as u32);
            entry.set_key(key);
            entry.set_mutation(mutation);
        }
    }
    Ok(serialize::write_message_to_words(&message))
}

/// Decode a frame produced by [`encode_accord_apply_v2`] (or the generic
/// [`encode_accord_envelope`]) and assert it is the multi-key Apply payload.
///
/// Fails loud on a frame that is a valid Accord envelope of a DIFFERENT kind: a
/// mis-routed Apply reply must never be silently reinterpreted as an Apply.
pub fn decode_accord_apply_v2(bytes: &[u8]) -> Result<AccordControlMessage, CapnpDecodeError> {
    match decode_accord_envelope(bytes)? {
        payload @ AccordControlMessage::ApplyV2 { .. } => Ok(payload),
        other => Err(CapnpDecodeError::UnknownPayload(format!(
            "expected an ApplyV2 payload, found a different Accord kind: {other:?}"
        ))),
    }
}

/// Region payload compression codec — one tag per codec already in the dependency set
/// (`lz4_flex`, `snap`, `zstd`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionCodec {
    /// No compression: the region is sent verbatim (the default).
    None,
    /// LZ4 block format: the cheap, fast end.
    Lz4,
    /// Snappy: fast, comparable to LZ4.
    Snap,
    /// Zstandard: the ratio end; honours [`RegionCompression::level`].
    Zstd,
}

impl RegionCodec {
    /// The wire tag for this codec.
    pub fn tag(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Lz4 => 1,
            Self::Snap => 2,
            Self::Zstd => 3,
        }
    }

    /// Decode a wire tag, failing loud on a codec this build does not know.
    pub fn from_tag(tag: u8) -> Result<Self, CapnpDecodeError> {
        Ok(match tag {
            0 => Self::None,
            1 => Self::Lz4,
            2 => Self::Snap,
            3 => Self::Zstd,
            other => {
                return Err(CapnpDecodeError::MalformedFrame(format!(
                    "unknown region compression codec tag {other}"
                )))
            }
        })
    }
}

impl std::fmt::Display for RegionCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
            Self::Snap => "snappy",
            Self::Zstd => "zstd",
        })
    }
}

/// The compression a coordinator applies to an Apply region.
///
/// `codec == None` is the default and reproduces the pre-compression wire exactly. The
/// other fields only matter when a codec is selected:
/// - `level` is the codec level where the codec has one (Zstd); the fixed-level codecs
///   ignore it.
/// - `block_bytes` is the block size the region is compressed in (0 = one block).
/// - `min_bytes` is the frame size below which no compression is attempted — on a small
///   frame the CPU cost plus the block headers lose to just sending the bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionCompression {
    pub codec: RegionCodec,
    pub level: i32,
    pub block_bytes: usize,
    pub min_bytes: usize,
}

impl RegionCompression {
    /// The default: no compression, byte-identical to the pre-compression wire.
    pub const NONE: Self = Self {
        codec: RegionCodec::None,
        level: 3,
        block_bytes: 0,
        min_bytes: usize::MAX,
    };

    /// Whether no compression will be applied.
    pub fn is_none(&self) -> bool {
        matches!(self.codec, RegionCodec::None)
    }
}

fn compress_block(
    codec: RegionCodec,
    level: i32,
    block: &[u8],
) -> Result<Vec<u8>, CapnpDecodeError> {
    Ok(match codec {
        RegionCodec::None => block.to_vec(),
        RegionCodec::Lz4 => lz4_flex::block::compress(block),
        RegionCodec::Snap => snap::raw::Encoder::new()
            .compress_vec(block)
            .map_err(|e| CapnpDecodeError::MalformedFrame(format!("snappy compress: {e}")))?,
        RegionCodec::Zstd => zstd::bulk::compress(block, level)
            .map_err(|e| CapnpDecodeError::MalformedFrame(format!("zstd compress: {e}")))?,
    })
}

fn decompress_block(
    codec: RegionCodec,
    block: &[u8],
    expected_len: usize,
) -> Result<Vec<u8>, CapnpDecodeError> {
    Ok(match codec {
        RegionCodec::None => block.to_vec(),
        RegionCodec::Lz4 => lz4_flex::block::decompress(block, expected_len)
            .map_err(|e| CapnpDecodeError::MalformedFrame(format!("lz4 decompress: {e}")))?,
        RegionCodec::Snap => snap::raw::Decoder::new()
            .decompress_vec(block)
            .map_err(|e| CapnpDecodeError::MalformedFrame(format!("snappy decompress: {e}")))?,
        RegionCodec::Zstd => zstd::bulk::decompress(block, expected_len)
            .map_err(|e| CapnpDecodeError::MalformedFrame(format!("zstd decompress: {e}")))?,
    })
}

/// Compress a region into a blocked, length-prefixed stream: for each block, a `u32`
/// little-endian compressed length followed by the compressed bytes. The block's
/// UNCOMPRESSED size is `block_bytes` except for the last block (which is the
/// remainder), so the decoder needs only `uncompressed_len` + `block_bytes` to read it.
fn compress_region(
    region: &[u8],
    codec: RegionCodec,
    level: i32,
    block_bytes: usize,
) -> Result<Vec<u8>, CapnpDecodeError> {
    let block_bytes = block_bytes.max(1);
    let mut out = Vec::with_capacity(region.len());
    let mut off = 0usize;
    while off < region.len() {
        let end = (off + block_bytes).min(region.len());
        let compressed = compress_block(codec, level, &region[off..end])?;
        let len = u32::try_from(compressed.len()).map_err(|_| {
            CapnpDecodeError::InvalidRequiredField(format!(
                "compressed block of {} bytes exceeds the u32 framing bound",
                compressed.len()
            ))
        })?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&compressed);
        off = end;
    }
    Ok(out)
}

fn decompress_region(
    payload: &[u8],
    codec: RegionCodec,
    uncompressed_len: usize,
    block_bytes: usize,
) -> Result<Vec<u8>, CapnpDecodeError> {
    let block_bytes = block_bytes.max(1);
    let mut out = Vec::with_capacity(uncompressed_len);
    let mut pos = 0usize;
    while pos < payload.len() {
        let len_bytes = payload.get(pos..pos + 4).ok_or_else(|| {
            CapnpDecodeError::MalformedFrame(
                "region block framing: truncated u32 length prefix".into(),
            )
        })?;
        let len = u32::from_le_bytes(len_bytes.try_into().expect("4 bytes")) as usize;
        pos += 4;
        let end = pos.checked_add(len).ok_or_else(|| {
            CapnpDecodeError::MalformedFrame("region block length overflows usize".into())
        })?;
        let block = payload.get(pos..end).ok_or_else(|| {
            CapnpDecodeError::MalformedFrame(format!(
                "region block {pos}..{end} escapes the {} byte payload",
                payload.len()
            ))
        })?;
        let remaining = uncompressed_len.saturating_sub(out.len());
        let expected = block_bytes.min(remaining);
        out.extend_from_slice(&decompress_block(codec, block, expected)?);
        pos = end;
    }
    if out.len() != uncompressed_len {
        return Err(CapnpDecodeError::MalformedFrame(format!(
            "region decompressed to {} bytes, expected {uncompressed_len}",
            out.len()
        )));
    }
    Ok(out)
}

/// Encode a multi-key Accord Apply as a region-REFERENCE frame, uncompressed.
///
/// The frame is a capnp HEADER (the transaction stamp plus a parallel
/// `offsets`/`lengths` index) followed by ONE contiguous REGION that concatenates the
/// borrowed `mutations`, in order — entry `i` is `region[offsets[i]..offsets[i]+lengths[i]]`.
///
/// This is the hot-path encoder for a transactional bulk load. The coordinator passes
/// the write-set's mutation slices as returned by
/// `WriteSetSpill::entry` (each a slice of the spill's memory map), and each entry's
/// bytes are appended to the region exactly ONCE, as a bulk copy — never into a capnp
/// struct, so there is no per-entry arena node and no per-entry small allocation. The
/// partition KEY is not carried: the frame is already scoped to the peer's keys and the
/// replica applies the mutation bytes in order.
pub fn encode_accord_apply_v2_region<'a, I>(
    txn_id: AccordTxnId,
    mutations: I,
) -> Result<Vec<u8>, CapnpDecodeError>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    encode_accord_apply_v2_region_with_compression(txn_id, mutations, RegionCompression::NONE)
}

/// [`encode_accord_apply_v2_region`] with an optional region compression.
///
/// When `cfg` selects a codec AND the region is at least `cfg.min_bytes`, the region is
/// compressed in `cfg.block_bytes` blocks (see [`compress_region`]); the header records
/// the codec tag, the uncompressed length and the block size so the peer can decompress
/// and address entries by the SAME `(offset, length)` index. If the compressed region is
/// not smaller than the raw one, or the region is below `min_bytes`, the frame is sent
/// uncompressed — compression must never inflate a frame.
pub fn encode_accord_apply_v2_region_with_compression<'a, I>(
    txn_id: AccordTxnId,
    mutations: I,
    cfg: RegionCompression,
) -> Result<Vec<u8>, CapnpDecodeError>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut offsets: Vec<u64> = Vec::new();
    let mut lengths: Vec<u32> = Vec::new();
    let mut region: Vec<u8> = Vec::new();
    for mutation in mutations {
        let len = u32::try_from(mutation.len()).map_err(|_| {
            CapnpDecodeError::InvalidRequiredField(format!(
                "accord apply region: entry of {} bytes exceeds the u32 length bound",
                mutation.len()
            ))
        })?;
        offsets.push(region.len() as u64);
        lengths.push(len);
        region.extend_from_slice(mutation);
    }
    let uncompressed_len = region.len() as u64;
    // Effective block size: the configured one, or the whole region (one block) when 0.
    let effective_block = if cfg.block_bytes == 0 {
        region.len().max(1)
    } else {
        cfg.block_bytes
    };
    let try_compress = !cfg.is_none() && !region.is_empty() && region.len() >= cfg.min_bytes;
    let (compression, block_bytes, payload) = if try_compress {
        let compressed = compress_region(&region, cfg.codec, cfg.level, effective_block)?;
        if compressed.len() < region.len() {
            (
                cfg.codec.tag(),
                u32::try_from(effective_block).unwrap_or(u32::MAX),
                compressed,
            )
        } else {
            // The codec did not shrink the region: never inflate a frame.
            (RegionCodec::None.tag(), 0u32, region)
        }
    } else {
        (RegionCodec::None.tag(), 0u32, region)
    };
    let mut header = encode_accord_envelope(&AccordControlMessage::ApplyV2Region {
        txn_id,
        offsets,
        lengths,
        compression,
        uncompressed_len,
        block_bytes,
    })?;
    header.extend_from_slice(&payload);
    Ok(header)
}

/// A BORROWED view of a region-REFERENCE Apply frame: the header's transaction stamp
/// plus an index into the appended region.
///
/// The region bytes are not copied out of the received body — [`Self::entry`] and
/// [`Self::mutations`] hand out `&[u8]` slices of it, so the peer decodes the frame by
/// OFFSET rather than parsing it into owned per-entry structures.
#[derive(Debug)]
pub struct AccordApplyV2RegionView<'a> {
    /// The transaction the write-set belongs to.
    pub txn_id: AccordTxnId,
    /// The region bytes. Borrowed from the received body when the frame was sent
    /// uncompressed; OWNED (decompressed into this view) when a codec was applied.
    region: std::borrow::Cow<'a, [u8]>,
    offsets: Vec<u64>,
    lengths: Vec<u32>,
    compression: RegionCodec,
}

impl<'a> AccordApplyV2RegionView<'a> {
    /// Number of write-set entries.
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Whether the frame carries no write-set entries.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// The raw region bytes (borrowed from the body, or the view's own decompressed
    /// buffer when the frame was compressed).
    pub fn region(&self) -> &[u8] {
        &self.region
    }

    /// The codec that was applied to the region ([`RegionCodec::None`] if uncompressed).
    pub fn compression(&self) -> RegionCodec {
        self.compression
    }

    /// Borrow entry `index` by its `(offset, length)` — a slice of the region.
    ///
    /// FAILS LOUD on an index that was never present and on a range that escapes the
    /// region: a corrupt index must never read past the received body.
    pub fn entry(&self, index: usize) -> Result<&[u8], CapnpDecodeError> {
        let (&offset, &len) = self
            .offsets
            .get(index)
            .zip(self.lengths.get(index))
            .ok_or_else(|| {
                CapnpDecodeError::InvalidRequiredField(format!(
                    "accord apply region: entry {index} is out of range ({} entries)",
                    self.offsets.len()
                ))
            })?;
        let start = usize::try_from(offset).map_err(|_| {
            CapnpDecodeError::InvalidRequiredField(format!(
                "accord apply region: entry {index} offset {offset} does not fit in usize"
            ))
        })?;
        let end = start.checked_add(len as usize).ok_or_else(|| {
            CapnpDecodeError::InvalidRequiredField(format!(
                "accord apply region: entry {index} range {start}..(start+{len}) overflows usize"
            ))
        })?;
        self.region.get(start..end).ok_or_else(|| {
            CapnpDecodeError::InvalidRequiredField(format!(
                "accord apply region: entry {index} range {start}..{end} escapes the {} byte region",
                self.region.len()
            ))
        })
    }

    /// Borrow every entry in write-set order.
    pub fn mutations(&self) -> impl Iterator<Item = Result<&[u8], CapnpDecodeError>> + '_ {
        (0..self.len()).map(move |index| self.entry(index))
    }

    /// The region length the index claims: `sum(lengths)`.
    pub fn indexed_region_len(&self) -> u64 {
        self.lengths.iter().map(|&len| u64::from(len)).sum()
    }

    /// FAIL LOUD unless the index is a self-consistent contiguous partition of the
    /// region: `offsets` and `lengths` parallel and non-empty, `offsets[i+1] ==
    /// offsets[i] + lengths[i]`, `offsets[0] == 0`, and the entries cover the region
    /// exactly. A header whose index does not cover the region is corrupt, and a peer
    /// must never apply a silently truncated or misaligned write-set.
    fn validate(&self) -> Result<(), CapnpDecodeError> {
        if self.offsets.len() != self.lengths.len() {
            return Err(CapnpDecodeError::InvalidRequiredField(format!(
                "accord apply region: {} offsets but {} lengths",
                self.offsets.len(),
                self.lengths.len()
            )));
        }
        let mut expected: u64 = 0;
        for (index, (&offset, &len)) in self.offsets.iter().zip(&self.lengths).enumerate() {
            if offset != expected {
                return Err(CapnpDecodeError::InvalidRequiredField(format!(
                    "accord apply region: entry {index} offset {offset} is not the contiguous \
                     position {expected}"
                )));
            }
            expected = expected.checked_add(u64::from(len)).ok_or_else(|| {
                CapnpDecodeError::InvalidRequiredField(format!(
                    "accord apply region: entry {index} makes the running length overflow u64"
                ))
            })?;
        }
        // Every entry must be in bounds (this also catches the LAST entry's end).
        for index in 0..self.len() {
            self.entry(index)?;
        }
        if expected != self.region.len() as u64 {
            return Err(CapnpDecodeError::InvalidRequiredField(format!(
                "accord apply region: index claims {expected} bytes but the region holds {}",
                self.region.len()
            )));
        }
        Ok(())
    }
}

/// Decode a frame produced by [`encode_accord_apply_v2_region`] into a BORROWED view.
///
/// The capnp header is read out of a `Cursor` to find where the message ends; the
/// region is everything after it. When the header names a codec the region is
/// decompressed into an owned buffer held by the view (see
/// [`decode_accord_apply_v2_region`]'s `Cow`); when it is uncompressed the view borrows
/// the received body directly. Either way the entries are read BY OFFSET — no owned
/// per-entry structure is produced.
pub fn decode_accord_apply_v2_region(
    bytes: &[u8],
) -> Result<AccordApplyV2RegionView<'_>, CapnpDecodeError> {
    // Find where the capnp header message ends: `read_message` consumes exactly one
    // message from the cursor, leaving the position at the region's first byte. The
    // reader is dropped before the position is read, so no message data is retained.
    let header_len = {
        let mut cursor = std::io::Cursor::new(bytes);
        let _reader = serialize::read_message(
            &mut cursor,
            message::ReaderOptions {
                traversal_limit_in_words: Some(8 * 1024 * 1024),
                nesting_limit: 64,
            },
        )?;
        usize::try_from(cursor.position()).map_err(|_| {
            CapnpDecodeError::MalformedFrame("region frame header length overflows usize".into())
        })?
    };
    let raw_payload = bytes.get(header_len..).ok_or_else(|| {
        CapnpDecodeError::MalformedFrame(format!(
            "region frame header of {header_len} bytes is longer than the {} byte body",
            bytes.len()
        ))
    })?;
    let (txn_id, offsets, lengths, codec, uncompressed_len, block_bytes) =
        match decode_accord_envelope(bytes)? {
            AccordControlMessage::ApplyV2Region {
                txn_id,
                offsets,
                lengths,
                compression,
                uncompressed_len,
                block_bytes,
            } => (
                txn_id,
                offsets,
                lengths,
                RegionCodec::from_tag(compression)?,
                uncompressed_len,
                block_bytes,
            ),
            other => {
                return Err(CapnpDecodeError::UnknownPayload(format!(
                    "expected an applyV2Region payload, found a different Accord kind: {other:?}"
                )))
            }
        };
    let uncompressed_len = usize::try_from(uncompressed_len).map_err(|_| {
        CapnpDecodeError::MalformedFrame("region uncompressed length overflows usize".into())
    })?;
    let region: std::borrow::Cow<'_, [u8]> = match codec {
        RegionCodec::None => {
            if raw_payload.len() != uncompressed_len {
                return Err(CapnpDecodeError::MalformedFrame(format!(
                    "region frame claims {uncompressed_len} bytes but carries {}",
                    raw_payload.len()
                )));
            }
            std::borrow::Cow::Borrowed(raw_payload)
        }
        _ => std::borrow::Cow::Owned(decompress_region(
            raw_payload,
            codec,
            uncompressed_len,
            block_bytes as usize,
        )?),
    };
    let view = AccordApplyV2RegionView {
        txn_id,
        region,
        offsets,
        lengths,
        compression: codec,
    };
    view.validate()?;
    Ok(view)
}

fn read_required_uuid(field: &str, data: &[u8]) -> Result<Uuid, CapnpDecodeError> {
    let id = read_optional_uuid(data)?.ok_or_else(|| {
        CapnpDecodeError::InvalidRequiredField(format!("{field} must be 16 bytes"))
    })?;
    validate_uuid(field, id)?;
    Ok(id)
}

fn read_optional_uuid(data: &[u8]) -> Result<Option<Uuid>, CapnpDecodeError> {
    if data.is_empty() {
        return Ok(None);
    }
    Uuid::from_slice(data).map(Some).map_err(|_| {
        CapnpDecodeError::InvalidRequiredField(format!(
            "uuid field must be 16 bytes, got {}",
            data.len()
        ))
    })
}

fn data_array<const N: usize>(data: &[u8]) -> Result<Option<[u8; N]>, CapnpDecodeError> {
    if data.is_empty() {
        return Ok(None);
    }
    data.try_into().map(Some).map_err(|_| {
        CapnpDecodeError::InvalidRequiredField(format!(
            "fixed data field must be {N} bytes, got {}",
            data.len()
        ))
    })
}

fn text_optional(text: capnp::text::Reader<'_>) -> Result<Option<String>, CapnpDecodeError> {
    let text = text.to_string()?;
    Ok(if text.is_empty() { None } else { Some(text) })
}

fn message_family_for_kind(kind: u16) -> MessageFamily {
    let Ok(kind) = u8::try_from(kind) else {
        return MessageFamily::Lifecycle;
    };
    match MsgType::try_from(kind) {
        Ok(MsgType::ClusterInvite | MsgType::ClusterInviteAck) => MessageFamily::ClusterControl,
        Ok(
            MsgType::RaftAppendEntries
            | MsgType::RaftAppendResponse
            | MsgType::RaftVote
            | MsgType::RaftVoteResponse
            | MsgType::RaftPreVote
            | MsgType::RaftPreVoteResponse
            | MsgType::RaftInstallSnapshot,
        ) => MessageFamily::Raft,
        Ok(
            MsgType::MutationForward
            | MsgType::MutationAck
            | MsgType::ReadRequest
            | MsgType::ReadResponse
            | MsgType::PartitionSuffixReadRequest
            | MsgType::RepairWrite
            | MsgType::RangeReadRequest
            | MsgType::RangeReadResponse
            | MsgType::TruncateForward
            | MsgType::TruncateAck
            | MsgType::RepairMerkleRequest
            | MsgType::RepairMerkleResponse
            | MsgType::RepairFetchRequest
            | MsgType::RepairFetchResponse
            | MsgType::RepairApplyRequest
            | MsgType::RepairApplyResponse
            | MsgType::ResultCursorPage
            | MsgType::ResultCursorPageReply,
        ) => MessageFamily::Data,
        Ok(
            MsgType::StreamStart
            | MsgType::StreamChunk
            | MsgType::StreamEnd
            | MsgType::SstableStreamStart
            | MsgType::SstableStreamChunk
            | MsgType::SstableStreamEnd
            | MsgType::RangeReadStreamRequest
            | MsgType::RangeReadStreamChunk
            | MsgType::RangeReadStreamHeartbeat
            | MsgType::RangeReadStreamDone
            | MsgType::RangeReadStreamCancel
            | MsgType::FulltextSearchStreamRequest
            | MsgType::FulltextSearchStreamChunk
            | MsgType::FulltextSearchStreamHeartbeat
            | MsgType::FulltextSearchStreamDone
            | MsgType::FulltextSearchStreamCancel,
        ) => MessageFamily::Stream,
        Ok(
            MsgType::PairWriteForward
            | MsgType::PairWriteAck
            | MsgType::PairCatchUp
            | MsgType::PairCatchUpResponse
            | MsgType::RoleSwap
            | MsgType::PairSchemaSync
            | MsgType::PairDdlForward
            | MsgType::PairDdlAck
            | MsgType::PairBatchForward
            | MsgType::PairBatchAck
            | MsgType::PairDissolve
            | MsgType::PairDissolveAck,
        ) => MessageFamily::Pair,
        Ok(MsgType::BatchlogWrite | MsgType::BatchlogDelete | MsgType::BatchlogReplay) => {
            MessageFamily::Batchlog
        }
        Ok(
            MsgType::IndexBuildRequest
            | MsgType::IndexBuildComplete
            | MsgType::FulltextSearchRequest
            | MsgType::FulltextSearchResponse
            | MsgType::IndexReadInPartitionRequest
            | MsgType::IndexReadInPartitionResponse,
        ) => MessageFamily::Index,
        Ok(
            MsgType::AccordPreAccept
            | MsgType::AccordPreAcceptOK
            | MsgType::AccordAccept
            | MsgType::AccordAcceptOK
            | MsgType::AccordCommit
            | MsgType::AccordRead
            | MsgType::AccordReadOK
            | MsgType::AccordApply
            | MsgType::AccordApplyOK
            | MsgType::AccordRecover
            | MsgType::AccordRecoverOK
            | MsgType::AccordPreAcceptV2
            | MsgType::AccordApplyV2
            | MsgType::AccordApplyV2Capnp
            | MsgType::AccordApplyV2Region,
        ) => MessageFamily::Accord,
        Ok(MsgType::BootstrapComplete | MsgType::BootstrapCompleteAck) => MessageFamily::Bootstrap,
        Ok(MsgType::ClusterMembershipForward | MsgType::ClusterMembershipForwardAck) => {
            MessageFamily::Membership
        }
        Ok(MsgType::Handshake | MsgType::HandshakeAck | MsgType::Ping | MsgType::Pong) | Err(_) => {
            MessageFamily::Lifecycle
        }
    }
}

fn nonzero(value: u64) -> Option<u64> {
    if value == 0 {
        None
    } else {
        Some(value)
    }
}

fn not_in_schema(err: capnp::NotInSchema) -> CapnpDecodeError {
    CapnpDecodeError::UnknownPayload(err.to_string())
}

impl From<CapnpErrorCode> for ErrorCode {
    fn from(value: CapnpErrorCode) -> Self {
        match value {
            CapnpErrorCode::MalformedFrame => Self::MalformedFrame,
            CapnpErrorCode::UnsupportedVersion => Self::UnsupportedVersion,
            CapnpErrorCode::UnsupportedFeature => Self::UnsupportedFeature,
            CapnpErrorCode::Unauthenticated => Self::Unauthenticated,
            CapnpErrorCode::Unauthorized => Self::Unauthorized,
            CapnpErrorCode::ClusterMismatch => Self::ClusterMismatch,
            CapnpErrorCode::UnknownMessage => Self::UnknownMessage,
            CapnpErrorCode::NoHandler => Self::NoHandler,
            CapnpErrorCode::Timeout => Self::Timeout,
            CapnpErrorCode::Overloaded => Self::Overloaded,
            CapnpErrorCode::NotLeader => Self::NotLeader,
            CapnpErrorCode::StaleEpoch => Self::StaleEpoch,
            CapnpErrorCode::Conflict => Self::Conflict,
            CapnpErrorCode::RetryRequired => Self::RetryRequired,
            CapnpErrorCode::FullBootstrapRequired => Self::FullBootstrapRequired,
            CapnpErrorCode::Internal => Self::Internal,
        }
    }
}

impl From<ErrorCode> for CapnpErrorCode {
    fn from(value: ErrorCode) -> Self {
        match value {
            ErrorCode::Ok => Self::Internal,
            ErrorCode::MalformedFrame => Self::MalformedFrame,
            ErrorCode::UnsupportedVersion => Self::UnsupportedVersion,
            ErrorCode::UnsupportedFeature => Self::UnsupportedFeature,
            ErrorCode::Unauthenticated => Self::Unauthenticated,
            ErrorCode::Unauthorized => Self::Unauthorized,
            ErrorCode::ClusterMismatch => Self::ClusterMismatch,
            ErrorCode::UnknownMessage => Self::UnknownMessage,
            ErrorCode::NoHandler => Self::NoHandler,
            ErrorCode::Timeout => Self::Timeout,
            ErrorCode::Overloaded => Self::Overloaded,
            ErrorCode::NotLeader => Self::NotLeader,
            ErrorCode::StaleEpoch => Self::StaleEpoch,
            ErrorCode::Conflict => Self::Conflict,
            ErrorCode::RetryRequired => Self::RetryRequired,
            ErrorCode::FullBootstrapRequired => Self::FullBootstrapRequired,
            ErrorCode::Internal => Self::Internal,
        }
    }
}

impl From<NodeLifecycleState> for envelope_capnp::NodeLifecycleState {
    fn from(value: NodeLifecycleState) -> Self {
        match value {
            NodeLifecycleState::Unknown => Self::Unknown,
            NodeLifecycleState::Joining => Self::Joining,
            NodeLifecycleState::Normal => Self::Normal,
            NodeLifecycleState::Leaving => Self::Leaving,
            NodeLifecycleState::Left => Self::Left,
            NodeLifecycleState::Degraded => Self::Degraded,
        }
    }
}

impl From<envelope_capnp::NodeLifecycleState> for NodeLifecycleState {
    fn from(value: envelope_capnp::NodeLifecycleState) -> Self {
        match value {
            envelope_capnp::NodeLifecycleState::Unknown => Self::Unknown,
            envelope_capnp::NodeLifecycleState::Joining => Self::Joining,
            envelope_capnp::NodeLifecycleState::Normal => Self::Normal,
            envelope_capnp::NodeLifecycleState::Leaving => Self::Leaving,
            envelope_capnp::NodeLifecycleState::Left => Self::Left,
            envelope_capnp::NodeLifecycleState::Degraded => Self::Degraded,
        }
    }
}

impl From<RecoveryReason> for envelope_capnp::RecoveryReason {
    fn from(value: RecoveryReason) -> Self {
        match value {
            RecoveryReason::Unknown => Self::Unknown,
            RecoveryReason::PeerReconnected => Self::PeerReconnected,
            RecoveryReason::StaleGeneration => Self::StaleGeneration,
            RecoveryReason::LostLocalState => Self::LostLocalState,
            RecoveryReason::RaftLogGap => Self::RaftLogGap,
            RecoveryReason::StreamRetry => Self::StreamRetry,
        }
    }
}

impl From<envelope_capnp::RecoveryReason> for RecoveryReason {
    fn from(value: envelope_capnp::RecoveryReason) -> Self {
        match value {
            envelope_capnp::RecoveryReason::Unknown => Self::Unknown,
            envelope_capnp::RecoveryReason::PeerReconnected => Self::PeerReconnected,
            envelope_capnp::RecoveryReason::StaleGeneration => Self::StaleGeneration,
            envelope_capnp::RecoveryReason::LostLocalState => Self::LostLocalState,
            envelope_capnp::RecoveryReason::RaftLogGap => Self::RaftLogGap,
            envelope_capnp::RecoveryReason::StreamRetry => Self::StreamRetry,
        }
    }
}

impl From<RecoveryAction> for envelope_capnp::RecoveryAction {
    fn from(value: RecoveryAction) -> Self {
        match value {
            RecoveryAction::NoOp => Self::NoOp,
            RecoveryAction::RefreshMetadata => Self::RefreshMetadata,
            RecoveryAction::ReplayRaft => Self::ReplayRaft,
            RecoveryAction::RunBootstrap => Self::RunBootstrap,
            RecoveryAction::FullBootstrapRequired => Self::FullBootstrapRequired,
        }
    }
}

impl From<envelope_capnp::RecoveryAction> for RecoveryAction {
    fn from(value: envelope_capnp::RecoveryAction) -> Self {
        match value {
            envelope_capnp::RecoveryAction::NoOp => Self::NoOp,
            envelope_capnp::RecoveryAction::RefreshMetadata => Self::RefreshMetadata,
            envelope_capnp::RecoveryAction::ReplayRaft => Self::ReplayRaft,
            envelope_capnp::RecoveryAction::RunBootstrap => Self::RunBootstrap,
            envelope_capnp::RecoveryAction::FullBootstrapRequired => Self::FullBootstrapRequired,
        }
    }
}

impl From<StreamKind> for envelope_capnp::StreamKind {
    fn from(value: StreamKind) -> Self {
        match value {
            StreamKind::Unknown => Self::Unknown,
            StreamKind::Sstable => Self::Sstable,
            StreamKind::RowFallback => Self::RowFallback,
        }
    }
}

impl From<envelope_capnp::StreamKind> for StreamKind {
    fn from(value: envelope_capnp::StreamKind) -> Self {
        match value {
            envelope_capnp::StreamKind::Unknown => Self::Unknown,
            envelope_capnp::StreamKind::Sstable => Self::Sstable,
            envelope_capnp::StreamKind::RowFallback => Self::RowFallback,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_current_protocol_slice() {
        assert_eq!(CURRENT_TRANSPORT_VERSION, 1);
        assert_eq!(MIN_SUPPORTED_TRANSPORT_VERSION, 1);
        assert_eq!(CURRENT_SCHEMA_VERSION, 1);
    }
}
