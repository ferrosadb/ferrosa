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

#[test]
fn bootstrap_row_fallback_limit_is_explicit_on_the_wire() {
    use ferrosa_net::protocol::{BootstrapControlMessage, BootstrapStreamPlan};

    let plan_id = uuid(0x90);
    let msg = envelope(CapnpPayload::Bootstrap(BootstrapControlMessage::Plan {
        plan_id,
        table_id: "ks.tbl".to_string(),
        plan: BootstrapStreamPlan::BoundedRows {
            row_fallback_limit: 64,
        },
    }));

    let decoded = decode_envelope(&encode_envelope(&msg).expect("bounded row plan encodes"))
        .expect("bounded row plan decodes");

    assert_eq!(decoded, msg);
    match decoded.payload {
        CapnpPayload::Bootstrap(BootstrapControlMessage::Plan {
            plan: BootstrapStreamPlan::BoundedRows { row_fallback_limit },
            ..
        }) => assert_eq!(row_fallback_limit, 64),
        other => panic!("expected bounded row fallback plan, got {other:?}"),
    }
}

#[test]
fn bootstrap_progress_and_completion_are_explicit_on_the_wire() {
    use ferrosa_net::protocol::BootstrapControlMessage;

    let plan_id = uuid(0x95);
    let progress = envelope(CapnpPayload::Bootstrap(BootstrapControlMessage::Progress {
        plan_id,
        completed_chunks: 8,
        total_chunks: 13,
        bytes_streamed: 65_536,
    }));
    assert_eq!(
        decode_envelope(&encode_envelope(&progress).expect("progress encodes"))
            .expect("progress decodes"),
        progress
    );

    let complete = envelope(CapnpPayload::Bootstrap(BootstrapControlMessage::Complete {
        plan_id,
        host: node(5, "127.0.0.5:7000"),
        bytes_streamed: 131_072,
    }));
    assert_eq!(
        decode_envelope(&encode_envelope(&complete).expect("complete encodes"))
            .expect("complete decodes"),
        complete
    );
}

#[test]
fn failed_sstable_stream_serializes_retry_not_row_materialization() {
    use ferrosa_net::protocol::{BootstrapControlMessage, BootstrapStreamPlan};

    let msg = envelope(CapnpPayload::Bootstrap(BootstrapControlMessage::Error {
        plan_id: uuid(0x91),
        failed_plan: BootstrapStreamPlan::SstableBulk {
            sstable_dir_count: 3,
        },
        retryable: true,
        safe_message: "sstable stream failed; retry required".to_string(),
    }));

    let decoded = decode_envelope(&encode_envelope(&msg).expect("stream error encodes"))
        .expect("stream error decodes");

    match decoded.payload {
        CapnpPayload::Bootstrap(BootstrapControlMessage::Error { failed_plan, .. }) => {
            assert_eq!(
                failed_plan,
                BootstrapStreamPlan::SstableBulk {
                    sstable_dir_count: 3,
                }
            );
            assert!(
                failed_plan.row_materialization_limit().is_none(),
                "SSTable failures must not imply any row materialization limit or unbounded row fallback"
            );
        }
        other => panic!("expected bootstrap error, got {other:?}"),
    }
}

#[test]
fn stream_chunk_metadata_is_fixed_and_bounded() {
    use ferrosa_net::protocol::{StreamChunkMetadata, StreamControlMessage, StreamKind};

    let chunk = envelope(CapnpPayload::Stream(StreamControlMessage::Chunk {
        metadata: StreamChunkMetadata {
            plan_id: uuid(0x92),
            kind: StreamKind::Sstable,
            chunk_index: 7,
            byte_offset: 8192,
            payload_bytes: 4,
            crc32c: 0xAABB_CCDD,
            is_last: false,
        },
        data: vec![1, 2, 3, 4],
    }));

    let decoded = decode_envelope(&encode_envelope(&chunk).expect("stream chunk encodes"))
        .expect("stream chunk decodes");

    assert_eq!(decoded, chunk);
}

#[test]
fn malformed_or_oversized_stream_chunks_fail_closed() {
    use ferrosa_net::protocol::{
        StreamChunkMetadata, StreamControlMessage, StreamKind, MAX_STREAM_CHUNK_BYTES,
    };

    let malformed = envelope(CapnpPayload::Stream(StreamControlMessage::Chunk {
        metadata: StreamChunkMetadata {
            plan_id: uuid(0x93),
            kind: StreamKind::Sstable,
            chunk_index: 0,
            byte_offset: 0,
            payload_bytes: 3,
            crc32c: 0,
            is_last: true,
        },
        data: vec![1, 2],
    }));
    assert!(matches!(
        encode_envelope(&malformed),
        Err(CapnpDecodeError::InvalidRequiredField(field)) if field.contains("payload_bytes")
    ));

    let oversized = envelope(CapnpPayload::Stream(StreamControlMessage::Chunk {
        metadata: StreamChunkMetadata {
            plan_id: uuid(0x94),
            kind: StreamKind::Sstable,
            chunk_index: 1,
            byte_offset: 0,
            payload_bytes: (MAX_STREAM_CHUNK_BYTES + 1) as u32,
            crc32c: 0,
            is_last: false,
        },
        data: vec![0; MAX_STREAM_CHUNK_BYTES + 1],
    }));
    assert!(matches!(
        encode_envelope(&oversized),
        Err(CapnpDecodeError::InvalidRequiredField(field)) if field.contains("stream chunk")
    ));
}
