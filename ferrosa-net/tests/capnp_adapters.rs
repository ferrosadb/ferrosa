use std::net::SocketAddr;

use ferrosa_net::protocol::{
    decode_envelope, encode_envelope, CapnpDecodeError, CapnpEnvelope, CapnpPayload,
    ClusterControlMessage, NodeIdentity, NodeLifecycleState, RecoveryAction,
    RecoveryControlMessage, RecoveryReason,
};
use uuid::Uuid;

fn uuid(byte: u8) -> Uuid {
    Uuid::from_bytes([byte; 16])
}

fn node(byte: u8, addr: &str) -> NodeIdentity {
    NodeIdentity {
        host_id: uuid(byte),
        node_id: byte as u64,
        address: addr.parse::<SocketAddr>().unwrap().to_string(),
        cql_broadcast: Some(format!("127.0.0.{byte}:19042")),
        data_center: "dc1".to_string(),
        rack: "rack1".to_string(),
        certificate_fingerprint: vec![byte; 32],
    }
}

fn envelope(payload: CapnpPayload) -> CapnpEnvelope {
    CapnpEnvelope {
        transport_version: 1,
        min_supported_transport_version: 1,
        schema_version: 1,
        required_features: 0,
        optional_features: 0,
        sender: node(1, "127.0.0.1:7000"),
        recipient: Some(node(2, "127.0.0.2:7000")),
        cluster_id: uuid(0xC1),
        epoch: 7,
        correlation_id: uuid(0xA1),
        causation_id: None,
        stream_id: 42,
        sequence: 0,
        deadline_unix_nanos: None,
        trace_id: [0x11; 16],
        span_id: [0x22; 8],
        trace_flags: 1,
        payload,
    }
}

#[test]
fn cluster_invite_and_ack_round_trip_through_capnp_adapters() {
    let invite_id = uuid(0x55);
    let invite = envelope(CapnpPayload::Cluster(ClusterControlMessage::Invite {
        initiator: node(1, "127.0.0.1:7000"),
        peers: vec![node(2, "127.0.0.2:7000"), node(3, "[::1]:7000")],
        formation_epoch: 99,
        expires_at_unix_nanos: 123_456_789,
        invite_id,
    }));

    let encoded = encode_envelope(&invite).expect("invite encodes");
    let decoded = decode_envelope(&encoded).expect("invite decodes");
    assert_eq!(decoded, invite);

    let ack = envelope(CapnpPayload::Cluster(ClusterControlMessage::InviteAck {
        host: node(2, "127.0.0.2:7000"),
        invite_id,
        accepted: false,
        reason: "stale formation epoch".to_string(),
    }));

    let encoded = encode_envelope(&ack).expect("ack encodes");
    let decoded = decode_envelope(&encoded).expect("ack decodes");
    assert_eq!(decoded, ack);
}

#[test]
fn rejoin_and_recovery_messages_round_trip_through_capnp_adapters() {
    let rejoin = envelope(CapnpPayload::Cluster(
        ClusterControlMessage::RejoinRequest {
            host: node(4, "127.0.0.4:7000"),
            last_known_membership_epoch: 12,
            last_applied_raft_index: 34,
            local_generation: 56,
            wants_bootstrap_plan: true,
        },
    ));
    assert_eq!(
        decode_envelope(&encode_envelope(&rejoin).unwrap()).unwrap(),
        rejoin
    );

    let plan_id = uuid(0x66);
    let recovery = envelope(CapnpPayload::Recovery(RecoveryControlMessage::Plan {
        plan_id,
        membership_epoch: 13,
        action: RecoveryAction::RunBootstrap,
        bootstrap_plan_id: Some(uuid(0x67)),
        leader: node(1, "127.0.0.1:7000"),
        safe_message: "bootstrap required after raft log gap".to_string(),
    }));
    assert_eq!(
        decode_envelope(&encode_envelope(&recovery).unwrap()).unwrap(),
        recovery
    );

    let progress = envelope(CapnpPayload::Recovery(RecoveryControlMessage::Progress {
        plan_id,
        phase: "streaming".to_string(),
        completed_units: 8,
        total_units: 10,
    }));
    assert_eq!(
        decode_envelope(&encode_envelope(&progress).unwrap()).unwrap(),
        progress
    );
}

#[test]
fn missing_optional_fields_decode_to_domain_defaults() {
    let invite = envelope(CapnpPayload::Cluster(ClusterControlMessage::Invite {
        initiator: NodeIdentity::minimal(uuid(1), "127.0.0.1:7000"),
        peers: vec![NodeIdentity::minimal(uuid(2), "127.0.0.2:7000")],
        formation_epoch: 1,
        expires_at_unix_nanos: 0,
        invite_id: uuid(0x77),
    }));

    let decoded = decode_envelope(&encode_envelope(&invite).unwrap()).unwrap();
    match decoded.payload {
        CapnpPayload::Cluster(ClusterControlMessage::Invite {
            initiator, peers, ..
        }) => {
            assert_eq!(initiator.cql_broadcast, None);
            assert!(initiator.data_center.is_empty());
            assert!(initiator.rack.is_empty());
            assert!(initiator.certificate_fingerprint.is_empty());
            assert_eq!(peers[0].cql_broadcast, None);
        }
        other => panic!("expected invite, got {other:?}"),
    }
}

#[test]
fn version_mismatch_and_malformed_frames_fail_closed() {
    let mut incompatible = envelope(CapnpPayload::Cluster(ClusterControlMessage::RejoinPlan {
        membership_epoch: 22,
        state: NodeLifecycleState::Joining,
        required_bootstrap: true,
        bootstrap_plan_id: Some(uuid(0x88)),
        peers: vec![node(1, "127.0.0.1:7000")],
        reason: "needs snapshot".to_string(),
    }));
    incompatible.transport_version = 2;
    incompatible.min_supported_transport_version = 2;
    let encoded = encode_envelope(&incompatible).unwrap();
    assert!(matches!(
        decode_envelope(&encoded),
        Err(CapnpDecodeError::UnsupportedVersion {
            transport_version: 2,
            min_supported_transport_version: 2
        })
    ));

    assert!(matches!(
        decode_envelope(b"not a capnp envelope"),
        Err(CapnpDecodeError::MalformedFrame(_))
    ));
}

#[test]
fn invalid_required_semantics_fail_closed() {
    let missing_required_host = envelope(CapnpPayload::Recovery(RecoveryControlMessage::Request {
        host: NodeIdentity::default(),
        reason: RecoveryReason::LostLocalState,
        last_membership_epoch: 3,
        last_applied_raft_index: 4,
        last_durable_commit_log_segment: 5,
        local_generation: 6,
    }));

    assert!(matches!(
        encode_envelope(&missing_required_host),
        Err(CapnpDecodeError::InvalidRequiredField(field)) if field.contains("host.host_id")
    ));
}
