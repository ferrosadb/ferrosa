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

pub fn encode_envelope(envelope: &CapnpEnvelope) -> Result<Vec<u8>, CapnpDecodeError> {
    validate_envelope(envelope)?;

    let mut message = message::Builder::new_default();
    {
        let mut root = message.init_root::<envelope::Builder>();
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
        CapnpPayload::Stream(_) | CapnpPayload::Error(_) | CapnpPayload::Legacy(_) => {}
    }
    match &envelope.payload {
        CapnpPayload::Cluster(msg) => validate_cluster(msg),
        CapnpPayload::Recovery(msg) => validate_recovery(msg),
        CapnpPayload::Bootstrap(msg) => validate_bootstrap(msg),
        CapnpPayload::Stream(msg) => validate_stream(msg),
        CapnpPayload::Error(_) | CapnpPayload::Legacy(_) => Ok(()),
    }
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
            | MsgType::RaftInstallSnapshot,
        ) => MessageFamily::Raft,
        Ok(
            MsgType::MutationForward
            | MsgType::MutationAck
            | MsgType::ReadRequest
            | MsgType::ReadResponse
            | MsgType::RepairWrite
            | MsgType::RangeReadRequest
            | MsgType::RangeReadResponse
            | MsgType::TruncateForward
            | MsgType::TruncateAck,
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
            | MsgType::RangeReadStreamCancel,
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
            | MsgType::PairBatchAck,
        ) => MessageFamily::Pair,
        Ok(MsgType::BatchlogWrite | MsgType::BatchlogDelete | MsgType::BatchlogReplay) => {
            MessageFamily::Batchlog
        }
        Ok(
            MsgType::IndexBuildRequest
            | MsgType::IndexBuildComplete
            | MsgType::IndexReadRequest
            | MsgType::IndexReadResponse,
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
            | MsgType::AccordRecoverOK,
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
