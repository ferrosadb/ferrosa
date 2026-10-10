use capnp::{message, serialize};
use ferrosa_net::codec::{MsgType, WireFrameFormat};
use ferrosa_net::message::Message;
use ferrosa_net::protocol::envelope_capnp::{cluster_control, envelope, MessageFamily};
use ferrosa_net::protocol::{
    decode_accord_envelope, decode_message_envelope, encode_accord_envelope,
    encode_message_envelope, negotiate_capnp_transport, AccordControlMessage, AccordReadPredicate,
    AccordTxnId, AccordWriteSetEntry, CapnpTransportMode,
};

#[test]
fn generated_envelope_exposes_stable_common_fields() {
    let mut message = message::Builder::new_default();
    {
        let mut envelope = message.init_root::<envelope::Builder>();
        envelope.set_magic(0x4645_5231);
        envelope.set_transport_version(1);
        envelope.set_min_supported_transport_version(1);
        envelope.set_schema_version(1);
        envelope.set_message_family(MessageFamily::Lifecycle);
        envelope.set_message_kind(0);
        envelope.set_required_features(1);
        envelope.set_optional_features(0);
        envelope.set_stream_id(42);
    }

    let words = serialize::write_message_to_words(&message);
    let reader = serialize::read_message_from_flat_slice(
        &mut words.as_slice(),
        message::ReaderOptions::new(),
    )
    .expect("generated envelope should decode from capnp words");
    let envelope = reader
        .get_root::<envelope::Reader>()
        .expect("generated envelope root should be readable");

    assert_eq!(envelope.get_magic(), 0x4645_5231);
    assert_eq!(envelope.get_transport_version(), 1);
    assert_eq!(envelope.get_min_supported_transport_version(), 1);
    assert_eq!(envelope.get_schema_version(), 1);
    assert_eq!(envelope.get_message_family(), Ok(MessageFamily::Lifecycle));
    assert_eq!(envelope.get_message_kind(), 0);
    assert_eq!(envelope.get_required_features(), 1);
    assert_eq!(envelope.get_stream_id(), 42);
}

#[test]
fn generated_cluster_invite_family_round_trips_without_legacy_message_migration() {
    let invite_id = [0x11_u8; 16];
    let mut message = message::Builder::new_default();
    {
        let mut envelope = message.init_root::<envelope::Builder>();
        envelope.set_magic(0x4645_5231);
        envelope.set_message_family(MessageFamily::ClusterControl);
        envelope.set_message_kind(0);
        let cluster = envelope.init_payload().init_cluster();
        let mut invite = cluster.init_op().init_invite();
        invite.set_formation_epoch(7);
        invite.set_expires_at_unix_nanos(1_234_567);
        invite
            .reborrow()
            .init_invite_id(invite_id.len() as u32)
            .copy_from_slice(&invite_id);
        invite.init_peers(0);
    }

    let words = serialize::write_message_to_words(&message);
    let reader = serialize::read_message_from_flat_slice(
        &mut words.as_slice(),
        message::ReaderOptions::new(),
    )
    .expect("generated cluster invite should decode from capnp words");
    let envelope = reader
        .get_root::<envelope::Reader>()
        .expect("generated envelope root should be readable");

    assert_eq!(
        envelope.get_message_family(),
        Ok(MessageFamily::ClusterControl)
    );
    let cluster = match envelope
        .get_payload()
        .which()
        .expect("payload union tag is known")
    {
        envelope::payload::Cluster(cluster) => cluster.expect("cluster payload is present"),
        _ => panic!("expected cluster-control payload"),
    };
    let invite = match cluster
        .get_op()
        .which()
        .expect("cluster op union tag is known")
    {
        cluster_control::op::Invite(invite) => invite.expect("invite payload is present"),
        _ => panic!("expected cluster invite op"),
    };

    assert_eq!(invite.get_formation_epoch(), 7);
    assert_eq!(invite.get_expires_at_unix_nanos(), 1_234_567);
    assert_eq!(
        invite.get_invite_id().expect("invite id is set"),
        &invite_id
    );
    assert_eq!(invite.get_peers().expect("peers list is set").len(), 0);
}

fn accord_txn(epoch: u64, time: u64, seq: u32, node: u64) -> AccordTxnId {
    AccordTxnId {
        epoch,
        time,
        seq,
        node,
    }
}

fn read_raw_envelope(frame: &[u8]) -> (Result<MessageFamily, capnp::NotInSchema>, u16) {
    let reader = serialize::read_message(
        &mut std::io::Cursor::new(frame),
        message::ReaderOptions::new(),
    )
    .expect("frame decodes as a capnp message");
    let envelope = reader
        .get_root::<envelope::Reader>()
        .expect("generated envelope root is readable");
    (envelope.get_message_family(), envelope.get_message_kind())
}

/// The Accord Apply family is the bulk data path. A capnp-encoded frame must
/// decode to EXACTLY the payload that produced it — including the two write-set
/// shapes the acceptance calls out: an EMPTY write-set and a SINGLE-entry one.
/// The frame must also classify as the Accord family with the matching kind.
#[test]
fn accord_apply_v2_roundtrips_empty_single_and_multi_write_sets() {
    let txn = accord_txn(0, 1_791_651_610_000_000_000, 3, 0x1122_3344_5566_7788);
    let cases: [Vec<AccordWriteSetEntry>; 3] = [
        vec![],
        vec![AccordWriteSetEntry {
            key: b"only-key".to_vec(),
            mutation: b"only-mutation".to_vec(),
        }],
        vec![
            AccordWriteSetEntry {
                key: b"key-alpha".to_vec(),
                mutation: b"mutation-for-alpha".to_vec(),
            },
            AccordWriteSetEntry {
                key: b"key-beta\x00with-nul".to_vec(),
                mutation: vec![0xABu8; 2048],
            },
        ],
    ];

    for writes in cases {
        let payload = AccordControlMessage::ApplyV2 {
            txn_id: txn,
            writes: writes.clone(),
        };
        let frame = encode_accord_envelope(&payload).expect("accord frame encodes");
        let (family, kind) = read_raw_envelope(&frame);
        assert_eq!(
            family,
            Ok(MessageFamily::Accord),
            "an Accord payload must classify as the Accord family"
        );
        assert_eq!(
            kind,
            MsgType::AccordApplyV2 as u16,
            "the frame's messageKind must be the bincode discriminant"
        );
        let decoded = decode_accord_envelope(&frame).expect("accord frame decodes");
        assert_eq!(
            decoded, payload,
            "the capnp frame must decode to exactly the payload that produced it"
        );
        assert_eq!(
            decoded,
            AccordControlMessage::ApplyV2 {
                txn_id: txn,
                writes
            }
        );
    }
}

/// Every Accord family message round-trips, and each classifies as Accord with
/// its own kind — the classification the family split already carries.
#[test]
fn accord_family_messages_roundtrip_and_classify() {
    let t = accord_txn(7, 1_000, 2, 11);
    let t2 = accord_txn(7, 1_250, 4, 22);
    let dep_a = accord_txn(7, 900, 1, 33);
    let dep_b = accord_txn(7, 950, 3, 44);

    let cases: Vec<(u16, AccordControlMessage)> = vec![
        (
            MsgType::AccordApply as u16,
            AccordControlMessage::Apply {
                txn_id: t,
                result_data: b"[applied]=true".to_vec(),
            },
        ),
        (
            MsgType::AccordApplyOK as u16,
            AccordControlMessage::ApplyOk { txn_id: t, from: 4 },
        ),
        (
            MsgType::AccordPreAccept as u16,
            AccordControlMessage::PreAccept {
                txn_id: t,
                t0: t2,
                key: b"pk-bytes".to_vec(),
                ballot: 42,
                epoch: 7,
            },
        ),
        (
            MsgType::AccordPreAcceptV2 as u16,
            AccordControlMessage::PreAcceptV2 {
                txn_id: t,
                t0: t2,
                keys: vec![b"k1".to_vec(), b"k2".to_vec()],
                ballot: 43,
                epoch: 7,
                snapshot_ts: Some(t2),
            },
        ),
        (
            MsgType::AccordPreAcceptOK as u16,
            AccordControlMessage::PreAcceptOk {
                from: 2,
                t: t2,
                deps: vec![dep_a, dep_b],
                snapshot_stale: true,
            },
        ),
        (
            MsgType::AccordAccept as u16,
            AccordControlMessage::Accept {
                txn_id: t,
                t0: t2,
                t: t2,
                deps: vec![dep_a],
                ballot: 44,
            },
        ),
        (
            MsgType::AccordAcceptOK as u16,
            AccordControlMessage::AcceptOk {
                txn_id: t,
                deps: vec![dep_a, dep_b],
            },
        ),
        (
            MsgType::AccordCommit as u16,
            AccordControlMessage::Commit {
                txn_id: t,
                t0: t2,
                t: t2,
                deps: vec![dep_b],
            },
        ),
        (
            MsgType::AccordRecover as u16,
            AccordControlMessage::Recover {
                txn_id: t,
                t0: t2,
                ballot: 45,
            },
        ),
        (
            MsgType::AccordRecoverOK as u16,
            AccordControlMessage::RecoverOk,
        ),
        (
            MsgType::AccordReadOK as u16,
            AccordControlMessage::ReadOk {
                txn_id: t,
                from: 3,
                condition_holds: false,
                current_row: b"existing-row".to_vec(),
            },
        ),
    ];

    for (kind, payload) in cases {
        let frame = encode_accord_envelope(&payload).expect("accord frame encodes");
        let (family, frame_kind) = read_raw_envelope(&frame);
        assert_eq!(family, Ok(MessageFamily::Accord));
        assert_eq!(frame_kind, kind, "kind mismatch for {payload:?}");
        let decoded = decode_accord_envelope(&frame).expect("accord frame decodes");
        assert_eq!(decoded, payload, "roundtrip mismatch for {payload:?}");
    }
}

/// A PreAcceptV2 with no snapshot timestamp must not gain one on the round trip,
/// and every read-vote predicate must survive the union intact.
#[test]
fn accord_optional_snapshot_and_read_predicates_roundtrip() {
    let t = accord_txn(7, 1_000, 2, 11);
    let t2 = accord_txn(7, 1_250, 4, 22);

    let no_snapshot = AccordControlMessage::PreAcceptV2 {
        txn_id: t,
        t0: t2,
        keys: vec![b"only".to_vec()],
        ballot: 1,
        epoch: 1,
        snapshot_ts: None,
    };
    let frame = encode_accord_envelope(&no_snapshot).unwrap();
    assert_eq!(decode_accord_envelope(&frame).unwrap(), no_snapshot);

    let predicates = [
        AccordReadPredicate::NotExists,
        AccordReadPredicate::ReadRow {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
        },
        AccordReadPredicate::SnapshotBarrier,
        AccordReadPredicate::Always,
        AccordReadPredicate::ReadClusteringRow {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            clustering: b"clustering-bytes\x00".to_vec(),
        },
    ];
    for predicate in predicates {
        let payload = AccordControlMessage::Read {
            txn_id: t,
            t: t2,
            key: b"read-vote-key".to_vec(),
            predicate: predicate.clone(),
        };
        let frame = encode_accord_envelope(&payload).unwrap();
        assert_eq!(
            decode_accord_envelope(&frame).unwrap(),
            payload,
            "read predicate {predicate:?} must round-trip"
        );
    }
}

/// A peer that has NOT advertised the Accord union capability must still receive
/// and decode the `Legacy` frame.
///
/// The generic message path cannot type the cluster crate's opaque Accord body,
/// so it wraps it as [`LegacyPayload`]; an old peer decodes it exactly as before.
/// And a peer that cannot decode the capnp envelope at all is negotiated down to
/// `WireFrameFormat::Legacy`.
#[test]
fn an_un_negotiated_peer_still_receives_and_decodes_the_legacy_frame() {
    let body = b"opaque-bincode-accord-body".to_vec();
    let msg = Message::AccordApplyV2(bytes::Bytes::from(body));
    let frame = encode_message_envelope(&msg, 7, uuid::Uuid::nil()).expect("legacy frame");

    let reader = serialize::read_message(&mut frame.as_slice(), message::ReaderOptions::new())
        .expect("frame decodes");
    let envelope = reader.get_root::<envelope::Reader>().expect("root");
    assert!(
        matches!(
            envelope.get_payload().which().expect("payload tag known"),
            envelope::payload::Legacy(_)
        ),
        "an untyped Accord message must ride the Legacy payload, not the accord union"
    );
    assert_eq!(envelope.get_message_family(), Ok(MessageFamily::Accord));

    let decoded = decode_message_envelope(&frame).expect("legacy frame decodes");
    assert_eq!(decoded.message, msg);
    assert_eq!(decoded.stream_id, 7);

    // A peer that cannot decode the capnp envelope falls back to Legacy.
    assert_eq!(
        negotiate_capnp_transport(CapnpTransportMode::PreferCapnp, 2, 2).unwrap(),
        WireFrameFormat::Legacy
    );
    assert!(
        negotiate_capnp_transport(CapnpTransportMode::RequireCapnp, 2, 2).is_err(),
        "RequireCapnp against a peer that cannot decode is an error, never a mis-framed send"
    );
}

/// The LIVE coordinator-side encoder writes each write-set entry straight from borrowed
/// `(key, mutation)` slices into the capnp arena (no intermediate owned `Vec`). The frame
/// it produces must be a standalone Accord `ApplyV2` envelope, ride the `AccordApplyV2Capnp`
/// wire type (0x7D, sent only to a `CAP_ACCORD_CAPNP` peer), and survive the full
/// `Message::encode`/`Message::decode` round trip byte-for-byte.
#[test]
fn borrowed_apply_v2_encoder_carries_the_capnp_wire_type_and_decodes_identically() {
    use ferrosa_net::protocol::{decode_accord_apply_v2, encode_accord_apply_v2};

    let txn = accord_txn(0, 1_791_651_610_000_000_000, 3, 0x1122_3344_5566_7788);
    let writes: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (b"key-1".to_vec(), vec![0x11u8; 512]),
        (b"key-2\x00with-nul".to_vec(), Vec::new()),
    ];
    let frame = encode_accord_apply_v2(
        txn,
        writes.iter().map(|(k, m)| (k.as_slice(), m.as_slice())),
    )
    .expect("borrowed apply v2 encodes");

    match decode_accord_apply_v2(&frame).expect("borrowed frame decodes") {
        AccordControlMessage::ApplyV2 {
            txn_id,
            writes: decoded,
        } => {
            assert_eq!(txn_id, txn);
            assert_eq!(decoded.len(), 2);
            assert_eq!(decoded[0].key, b"key-1");
            assert_eq!(decoded[0].mutation, vec![0x11u8; 512]);
            assert_eq!(decoded[1].key, b"key-2\x00with-nul");
            assert!(decoded[1].mutation.is_empty());
        }
        other => panic!("expected an ApplyV2 payload, got {other:?}"),
    }

    let msg = Message::AccordApplyV2Capnp(bytes::Bytes::from(frame.clone()));
    assert_eq!(msg.msg_type(), MsgType::AccordApplyV2Capnp);
    let mut body = bytes::BytesMut::new();
    msg.encode(&mut body).expect("message encodes");
    match Message::decode(MsgType::AccordApplyV2Capnp, &mut body.freeze()).expect("message decodes")
    {
        Message::AccordApplyV2Capnp(decoded) => assert_eq!(decoded.as_ref(), frame.as_slice()),
        other => panic!("expected AccordApplyV2Capnp, got {other:?}"),
    }
}
