use bytes::{Bytes, BytesMut};
use ferrosa_net::codec::{Frame, FrameHeader, InternodeCodec, Lane, MsgType, WireFrameFormat};
use ferrosa_net::protocol::{
    decode_envelope, encode_envelope, negotiate_capnp_capabilities, validate_capnp_capabilities,
    BootstrapControlMessage, BootstrapStreamPlan, CapnpDecodeError, CapnpEnvelope, CapnpPayload,
    CapnpTransportMode, ClusterControlMessage, NodeIdentity, NodeLifecycleState,
    CURRENT_TRANSPORT_VERSION, MIN_SUPPORTED_TRANSPORT_VERSION,
};
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

const FEATURE_CLUSTER_INVITE_V2: u64 = 0x04;
const FEATURE_BOOTSTRAP_CONTROL: u64 = 0x08;
const FEATURE_EXPERIMENTAL: u64 = 0x8000_0000_0000_0000;

fn uuid(byte: u8) -> Uuid {
    Uuid::from_bytes([byte; 16])
}

fn node(byte: u8, address: &str) -> NodeIdentity {
    NodeIdentity {
        host_id: uuid(byte),
        node_id: byte as u64,
        address: address.to_string(),
        cql_broadcast: Some(format!("127.0.0.{byte}:9042")),
        data_center: "dc1".to_string(),
        rack: format!("rack{byte}"),
        certificate_fingerprint: vec![byte; 32],
    }
}

fn envelope(
    sender: NodeIdentity,
    recipient: Option<NodeIdentity>,
    payload: CapnpPayload,
) -> CapnpEnvelope {
    CapnpEnvelope {
        transport_version: CURRENT_TRANSPORT_VERSION,
        min_supported_transport_version: MIN_SUPPORTED_TRANSPORT_VERSION,
        schema_version: 1,
        required_features: FEATURE_BOOTSTRAP_CONTROL,
        optional_features: FEATURE_CLUSTER_INVITE_V2,
        sender,
        recipient,
        cluster_id: uuid(0xC1),
        epoch: 44,
        correlation_id: uuid(0xA0),
        causation_id: None,
        stream_id: 7,
        sequence: 0,
        deadline_unix_nanos: None,
        trace_id: [0xAB; 16],
        span_id: [0xCD; 8],
        trace_flags: 1,
        payload,
    }
}

fn capnp_frame_bytes(body: Vec<u8>, msg_type: MsgType, stream_id: u32) -> Vec<u8> {
    let mut codec = InternodeCodec::with_format(4096, WireFrameFormat::CapnpEnvelope);
    let frame = Frame {
        header: FrameHeader::new_with_format(
            WireFrameFormat::CapnpEnvelope,
            msg_type,
            Lane::Raft,
            stream_id,
            0,
        ),
        body: Bytes::from(body),
    };
    let mut encoded = BytesMut::new();
    codec.encode(frame, &mut encoded).expect("frame encodes");
    encoded.to_vec()
}

#[test]
fn golden_invite_frame_fixture_is_stable_and_decodable() {
    let invite = envelope(
        node(1, "127.0.0.1:7000"),
        Some(node(2, "127.0.0.2:7000")),
        CapnpPayload::Cluster(ClusterControlMessage::Invite {
            initiator: node(1, "127.0.0.1:7000"),
            peers: vec![node(2, "127.0.0.2:7000"), node(3, "127.0.0.3:7000")],
            formation_epoch: 44,
            expires_at_unix_nanos: 1_778_800_000_000_000_000,
            invite_id: uuid(0x55),
        }),
    );
    let expected = include_bytes!("fixtures/capnp/invite_frame_v1.bin");
    let actual = capnp_frame_bytes(
        encode_envelope(&invite).expect("invite envelope encodes"),
        MsgType::ClusterInvite,
        7,
    );

    assert_eq!(actual.as_slice(), expected);

    let mut codec = InternodeCodec::with_format(4096, WireFrameFormat::CapnpEnvelope);
    let decoded_frame = codec
        .decode(&mut BytesMut::from(expected.as_slice()))
        .expect("golden frame decodes")
        .expect("fixture contains a full frame");
    assert_eq!(decoded_frame.header.msg_type, MsgType::ClusterInvite);
    assert_eq!(
        decode_envelope(&decoded_frame.body).expect("golden envelope decodes"),
        invite
    );
}

#[test]
fn golden_bootstrap_plan_frame_fixture_is_stable_and_decodable() {
    let plan = envelope(
        node(2, "127.0.0.2:7000"),
        Some(node(3, "127.0.0.3:7000")),
        CapnpPayload::Bootstrap(BootstrapControlMessage::Plan {
            plan_id: uuid(0x90),
            table_id: "ks.tbl".to_string(),
            plan: BootstrapStreamPlan::SstableBulk {
                sstable_dir_count: 3,
            },
        }),
    );
    let expected = include_bytes!("fixtures/capnp/bootstrap_plan_frame_v1.bin");
    let actual = capnp_frame_bytes(
        encode_envelope(&plan).expect("bootstrap envelope encodes"),
        MsgType::BootstrapComplete,
        9,
    );

    assert_eq!(actual.as_slice(), expected);

    let mut codec = InternodeCodec::with_format(4096, WireFrameFormat::CapnpEnvelope);
    let decoded_frame = codec
        .decode(&mut BytesMut::from(expected.as_slice()))
        .expect("golden frame decodes")
        .expect("fixture contains a full frame");
    assert_eq!(
        decode_envelope(&decoded_frame.body).expect("golden envelope decodes"),
        plan
    );
}

#[test]
fn malformed_golden_frame_mutations_are_rejected_without_legacy_fallback() {
    let fixture = include_bytes!("fixtures/capnp/invite_frame_v1.bin");

    let mut bad_magic = BytesMut::from(fixture.as_slice());
    let body_offset = ferrosa_net::codec::HEADER_SIZE;
    bad_magic[body_offset + 16] ^= 0xFF;
    let mut codec = InternodeCodec::with_format(4096, WireFrameFormat::CapnpEnvelope);
    let frame = codec
        .decode(&mut bad_magic)
        .expect("header still decodes")
        .expect("fixture contains a full frame");
    assert!(matches!(
        decode_envelope(&frame.body),
        Err(CapnpDecodeError::MalformedFrame(reason)) if reason.contains("bad magic")
    ));

    let mut legacy_version = BytesMut::from(fixture.as_slice());
    legacy_version[0] = ferrosa_net::codec::LEGACY_FRAME_VERSION;
    let mut codec = InternodeCodec::with_format(4096, WireFrameFormat::CapnpEnvelope);
    assert!(codec.decode(&mut legacy_version).is_err());

    for cut in 0..fixture.len() {
        let mut truncated = BytesMut::from(&fixture[..cut]);
        let mut codec = InternodeCodec::with_format(4096, WireFrameFormat::CapnpEnvelope);
        let decoded = codec.decode(&mut truncated);
        assert!(
            matches!(decoded, Ok(None) | Err(_)),
            "truncated fixture unexpectedly produced a frame at cut {cut}"
        );
    }
}

#[test]
fn version_and_feature_negotiation_matrix_is_explicit() {
    let supported = FEATURE_BOOTSTRAP_CONTROL | FEATURE_CLUSTER_INVITE_V2;

    let cases = [
        (
            CapnpTransportMode::LegacyOnly,
            1,
            1,
            supported,
            Ok(WireFrameFormat::Legacy),
        ),
        (
            CapnpTransportMode::PreferCapnp,
            1,
            1,
            supported,
            Ok(WireFrameFormat::CapnpEnvelope),
        ),
        (
            CapnpTransportMode::PreferCapnp,
            0,
            0,
            supported,
            Ok(WireFrameFormat::Legacy),
        ),
        (
            CapnpTransportMode::RequireCapnp,
            1,
            1,
            supported,
            Ok(WireFrameFormat::CapnpEnvelope),
        ),
        (
            CapnpTransportMode::RequireCapnp,
            2,
            2,
            supported,
            Err(CapnpDecodeError::UnsupportedVersion {
                transport_version: 2,
                min_supported_transport_version: 2,
            }),
        ),
        (
            CapnpTransportMode::RequireCapnp,
            1,
            1,
            FEATURE_EXPERIMENTAL,
            Err(CapnpDecodeError::UnsupportedFeature {
                missing_features: FEATURE_EXPERIMENTAL,
            }),
        ),
    ];

    for (mode, peer_min, peer_max, peer_required, expected) in cases {
        let result =
            negotiate_capnp_capabilities(mode, peer_min, peer_max, supported, peer_required)
                .map(|negotiated| negotiated.frame_format);
        assert_eq!(result, expected);
    }
}

#[test]
fn envelope_required_features_are_validated_before_dispatch() {
    let mut msg = envelope(
        node(1, "127.0.0.1:7000"),
        Some(node(2, "127.0.0.2:7000")),
        CapnpPayload::Cluster(ClusterControlMessage::RejoinPlan {
            membership_epoch: 45,
            state: NodeLifecycleState::Joining,
            required_bootstrap: true,
            bootstrap_plan_id: Some(uuid(0x90)),
            peers: vec![node(1, "127.0.0.1:7000"), node(2, "127.0.0.2:7000")],
            reason: "needs bounded SSTable stream".to_string(),
        }),
    );
    msg.required_features = FEATURE_BOOTSTRAP_CONTROL | FEATURE_EXPERIMENTAL;

    assert_eq!(
        validate_capnp_capabilities(&msg, FEATURE_BOOTSTRAP_CONTROL | FEATURE_CLUSTER_INVITE_V2),
        Err(CapnpDecodeError::UnsupportedFeature {
            missing_features: FEATURE_EXPERIMENTAL,
        })
    );
}

#[test]
fn three_node_invite_rejoin_and_bootstrap_wire_smoke() {
    let n1 = node(1, "127.0.0.1:7000");
    let n2 = node(2, "127.0.0.2:7000");
    let n3 = node(3, "127.0.0.3:7000");
    let supported = FEATURE_BOOTSTRAP_CONTROL | FEATURE_CLUSTER_INVITE_V2;

    let negotiated = negotiate_capnp_capabilities(
        CapnpTransportMode::RequireCapnp,
        CURRENT_TRANSPORT_VERSION,
        CURRENT_TRANSPORT_VERSION,
        supported,
        FEATURE_BOOTSTRAP_CONTROL,
    )
    .expect("nodes agree on CapnProto transport");
    assert_eq!(negotiated.frame_format, WireFrameFormat::CapnpEnvelope);
    assert_eq!(negotiated.enabled_features, FEATURE_BOOTSTRAP_CONTROL);

    let invite = envelope(
        n1.clone(),
        Some(n2.clone()),
        CapnpPayload::Cluster(ClusterControlMessage::Invite {
            initiator: n1.clone(),
            peers: vec![n2.clone(), n3.clone()],
            formation_epoch: 44,
            expires_at_unix_nanos: 1_778_800_000_000_000_000,
            invite_id: uuid(0x55),
        }),
    );
    assert_eq!(
        decode_envelope(&encode_envelope(&invite).unwrap()).unwrap(),
        invite
    );

    let rejoin = envelope(
        n3.clone(),
        Some(n1.clone()),
        CapnpPayload::Cluster(ClusterControlMessage::RejoinRequest {
            host: n3.clone(),
            last_known_membership_epoch: 43,
            last_applied_raft_index: 1024,
            local_generation: 7,
            wants_bootstrap_plan: true,
        }),
    );
    assert_eq!(
        decode_envelope(&encode_envelope(&rejoin).unwrap()).unwrap(),
        rejoin
    );

    let bootstrap = envelope(
        n1.clone(),
        Some(n3.clone()),
        CapnpPayload::Bootstrap(BootstrapControlMessage::Plan {
            plan_id: uuid(0x90),
            table_id: "ks.tbl".to_string(),
            plan: BootstrapStreamPlan::BoundedRows {
                row_fallback_limit: 128,
            },
        }),
    );
    assert_eq!(
        decode_envelope(&encode_envelope(&bootstrap).unwrap()).unwrap(),
        bootstrap
    );
}
