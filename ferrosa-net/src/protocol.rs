//! Generated Cap'n Proto protocol modules and adapters for internode messages.
//!
//! Live networking still uses the legacy [`crate::message::Message`] enum.  The
//! adapter types below are an explicit domain-facing seam for the Cap'n Proto
//! envelope/body contract so callers do not depend on generated wire structs.

use std::fmt;

use capnp::{message, serialize};
use uuid::Uuid;

use crate::protocol::envelope_capnp::{
    cluster_control, envelope, node_identity, recovery_control, MessageFamily,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapnpPayload {
    Cluster(ClusterControlMessage),
    Recovery(RecoveryControlMessage),
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
        }
    }

    Ok(serialize::write_message_to_words(&message))
}

pub fn decode_envelope(mut bytes: &[u8]) -> Result<CapnpEnvelope, CapnpDecodeError> {
    let reader = serialize::read_message_from_flat_slice(
        &mut bytes,
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

fn validate_envelope(envelope: &CapnpEnvelope) -> Result<(), CapnpDecodeError> {
    validate_node("sender", &envelope.sender)?;
    match &envelope.payload {
        CapnpPayload::Cluster(msg) => validate_cluster(msg),
        CapnpPayload::Recovery(msg) => validate_recovery(msg),
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

fn read_nodes(
    reader: capnp::struct_list::Reader<'_, node_identity::Owned>,
) -> Result<Vec<NodeIdentity>, CapnpDecodeError> {
    (0..reader.len())
        .map(|idx| read_node(reader.get(idx)))
        .collect()
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
