use bytes::{Bytes, BytesMut};
use ferrosa_net::codec::{
    Frame, FrameHeader, InternodeCodec, Lane, MsgType, WireFrameFormat, CAPNP_FRAME_VERSION,
};
use ferrosa_net::message::Message;
use ferrosa_net::protocol::{
    decode_message_envelope, encode_error_envelope, encode_message_envelope,
    negotiate_capnp_transport, CapnpDecodeError, CapnpErrorCode, CapnpPayload, CapnpTransportMode,
    CURRENT_TRANSPORT_VERSION,
};
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

fn uuid(byte: u8) -> Uuid {
    Uuid::from_bytes([byte; 16])
}

#[test]
fn compatibility_gate_negotiates_or_fails_explicitly() {
    assert_eq!(
        negotiate_capnp_transport(CapnpTransportMode::PreferCapnp, 1, 2).unwrap(),
        WireFrameFormat::CapnpEnvelope
    );
    assert_eq!(
        negotiate_capnp_transport(CapnpTransportMode::PreferCapnp, 0, 0).unwrap(),
        WireFrameFormat::Legacy
    );

    assert!(matches!(
        negotiate_capnp_transport(CapnpTransportMode::RequireCapnp, 0, 0),
        Err(CapnpDecodeError::UnsupportedVersion {
            transport_version: 0,
            min_supported_transport_version: 0
        })
    ));
}

#[test]
fn capnp_message_envelope_preserves_stream_correlation_and_legacy_message_body() {
    let msg = Message::Ping {
        nonce: 42,
        sent_at: 123,
    };
    let correlation_id = uuid(0xA7);

    let encoded =
        encode_message_envelope(&msg, 77, correlation_id).expect("message envelope encodes");
    let decoded = decode_message_envelope(&encoded).expect("message envelope decodes");

    assert_eq!(decoded.stream_id, 77);
    assert_eq!(decoded.correlation_id, correlation_id);
    assert_eq!(decoded.message, msg);
}

#[test]
fn capnp_error_envelope_round_trips_with_failed_correlation_id() {
    let failed = uuid(0xB8);
    let encoded = encode_error_envelope(
        CapnpErrorCode::UnsupportedVersion,
        false,
        "peer requires unsupported CapnProto transport version",
        MsgType::Ping,
        failed,
        CURRENT_TRANSPORT_VERSION,
        CURRENT_TRANSPORT_VERSION,
    )
    .expect("error envelope encodes");

    let decoded = ferrosa_net::protocol::decode_envelope(&encoded).expect("error envelope decodes");
    match decoded.payload {
        CapnpPayload::Error(err) => {
            assert_eq!(err.code, CapnpErrorCode::UnsupportedVersion);
            assert!(!err.retryable);
            assert_eq!(
                err.safe_message,
                "peer requires unsupported CapnProto transport version"
            );
            assert_eq!(err.failed_kind, MsgType::Ping as u16);
            assert_eq!(err.failed_correlation_id, Some(failed));
        }
        other => panic!("expected error payload, got {other:?}"),
    }
}

#[test]
fn capnp_envelope_frames_use_explicit_header_version_and_reject_mismatch() {
    let mut codec = InternodeCodec::with_format(1024, WireFrameFormat::CapnpEnvelope);
    let frame = Frame {
        header: FrameHeader::new_with_format(
            WireFrameFormat::CapnpEnvelope,
            MsgType::Ping,
            Lane::Raft,
            9,
            0,
        ),
        body: Bytes::from_static(b"capnp-body"),
    };

    let mut encoded = BytesMut::new();
    codec
        .encode(frame, &mut encoded)
        .expect("capnp frame encodes");
    assert_eq!(encoded[0], CAPNP_FRAME_VERSION);

    let decoded = codec
        .decode(&mut encoded)
        .expect("capnp frame decodes")
        .unwrap();
    assert_eq!(decoded.header.version, CAPNP_FRAME_VERSION);
    assert_eq!(decoded.header.stream_id, 9);
    assert_eq!(decoded.body, Bytes::from_static(b"capnp-body"));

    let mut legacy = BytesMut::new();
    FrameHeader::new(MsgType::Ping, Lane::Raft, 9, 4).encode(&mut legacy);
    legacy.extend_from_slice(b"ping");
    assert!(matches!(
        codec.decode(&mut legacy),
        Err(ferrosa_net::error::NetError::Protocol(reason)) if reason.contains("legacy frame received on CapnProto envelope connection")
    ));
}

#[test]
fn partial_or_truncated_capnp_envelope_frames_fail_closed_without_silent_fallback() {
    let msg = Message::Ping {
        nonce: 7,
        sent_at: 8,
    };
    let encoded = encode_message_envelope(&msg, 3, uuid(0xCC)).expect("message envelope encodes");

    for cut in 0..encoded.len() {
        let result = decode_message_envelope(&encoded[..cut]);
        if cut < encoded.len() {
            assert!(
                matches!(result, Err(CapnpDecodeError::MalformedFrame(_))),
                "cut {cut} unexpectedly decoded"
            );
        }
    }
}
