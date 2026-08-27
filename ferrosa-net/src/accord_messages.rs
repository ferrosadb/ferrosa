/// Accord protocol message payloads.
/// These are serialized as opaque Bytes in the ferrosa-net Message enum,
/// consistent with how Raft and Mutation messages are handled.
///
/// Accord message type discriminant for higher-level routing.
/// ferrosa-cluster uses this to dispatch Accord messages to the correct handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AccordMessageType {
    PreAccept,
    PreAcceptOK,
    Accept,
    AcceptOK,
    Commit,
    Read,
    ReadOK,
    Apply,
    ApplyOK,
    Recover,
    RecoverOK,
}

impl AccordMessageType {
    /// All Accord message types, useful for iteration in tests.
    pub const ALL: [AccordMessageType; 11] = [
        AccordMessageType::PreAccept,
        AccordMessageType::PreAcceptOK,
        AccordMessageType::Accept,
        AccordMessageType::AcceptOK,
        AccordMessageType::Commit,
        AccordMessageType::Read,
        AccordMessageType::ReadOK,
        AccordMessageType::Apply,
        AccordMessageType::ApplyOK,
        AccordMessageType::Recover,
        AccordMessageType::RecoverOK,
    ];
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use crate::codec::MsgType;
    use crate::message::Message;

    use super::*;

    /// Helper: returns (MsgType, Message constructor) for each Accord variant.
    fn accord_variants(payload: Bytes) -> Vec<(MsgType, Message)> {
        vec![
            (
                MsgType::AccordPreAccept,
                Message::AccordPreAccept(payload.clone()),
            ),
            (
                MsgType::AccordPreAcceptOK,
                Message::AccordPreAcceptOK(payload.clone()),
            ),
            (
                MsgType::AccordAccept,
                Message::AccordAccept(payload.clone()),
            ),
            (
                MsgType::AccordAcceptOK,
                Message::AccordAcceptOK(payload.clone()),
            ),
            (
                MsgType::AccordCommit,
                Message::AccordCommit(payload.clone()),
            ),
            (MsgType::AccordRead, Message::AccordRead(payload.clone())),
            (
                MsgType::AccordReadOK,
                Message::AccordReadOK(payload.clone()),
            ),
            (MsgType::AccordApply, Message::AccordApply(payload.clone())),
            (
                MsgType::AccordApplyOK,
                Message::AccordApplyOK(payload.clone()),
            ),
            (
                MsgType::AccordRecover,
                Message::AccordRecover(payload.clone()),
            ),
            (MsgType::AccordRecoverOK, Message::AccordRecoverOK(payload)),
        ]
    }

    #[test]
    fn accord_message_roundtrip() {
        let payload = Bytes::from_static(b"accord-test-payload-data");
        for (msg_type, msg) in accord_variants(payload.clone()) {
            assert_eq!(
                msg.msg_type(),
                msg_type,
                "msg_type() mismatch for {msg_type:?}"
            );

            let mut buf = BytesMut::new();
            msg.encode(&mut buf).expect("encode should succeed");
            let decoded =
                Message::decode(msg_type, &mut buf.freeze()).expect("decode should succeed");
            assert_eq!(decoded, msg, "roundtrip mismatch for {msg_type:?}");
        }
    }

    #[test]
    fn accord_v2_multikey_message_roundtrip() {
        // The additive multi-key V2 variants encode/decode through the codec and
        // report the right MsgType, exactly like the single-key family.
        let payload = Bytes::from_static(b"multikey-v2-opaque-payload");
        let variants = [
            (
                MsgType::AccordPreAcceptV2,
                Message::AccordPreAcceptV2(payload.clone()),
            ),
            (
                MsgType::AccordApplyV2,
                Message::AccordApplyV2(payload.clone()),
            ),
        ];
        for (msg_type, msg) in variants {
            assert_eq!(
                msg.msg_type(),
                msg_type,
                "msg_type() mismatch for {msg_type:?}"
            );
            let mut buf = BytesMut::new();
            msg.encode(&mut buf).expect("encode should succeed");
            let decoded =
                Message::decode(msg_type, &mut buf.freeze()).expect("decode should succeed");
            assert_eq!(decoded, msg, "roundtrip mismatch for {msg_type:?}");
        }
        // V2 codes are distinct from each other and from the single-key Apply.
        assert_ne!(
            MsgType::AccordPreAcceptV2 as u8,
            MsgType::AccordApplyV2 as u8
        );
        assert_ne!(MsgType::AccordApplyV2 as u8, MsgType::AccordApply as u8);
    }

    #[test]
    fn accord_message_type_codes_unique() {
        let accord_codes: Vec<u8> = vec![
            MsgType::AccordPreAccept as u8,
            MsgType::AccordPreAcceptOK as u8,
            MsgType::AccordAccept as u8,
            MsgType::AccordAcceptOK as u8,
            MsgType::AccordCommit as u8,
            MsgType::AccordRead as u8,
            MsgType::AccordReadOK as u8,
            MsgType::AccordApply as u8,
            MsgType::AccordApplyOK as u8,
            MsgType::AccordRecover as u8,
            MsgType::AccordRecoverOK as u8,
        ];

        // All 11 Accord codes are distinct
        let mut sorted = accord_codes.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            accord_codes.len(),
            "Accord MsgType codes are not all unique"
        );

        // No collision with existing (pre-Accord) MsgType codes
        let existing_codes: Vec<u8> = vec![
            MsgType::Handshake as u8,
            MsgType::HandshakeAck as u8,
            MsgType::Ping as u8,
            MsgType::Pong as u8,
            MsgType::RaftAppendEntries as u8,
            MsgType::RaftAppendResponse as u8,
            MsgType::RaftVote as u8,
            MsgType::RaftVoteResponse as u8,
            MsgType::RaftInstallSnapshot as u8,
            MsgType::MutationForward as u8,
            MsgType::MutationAck as u8,
            MsgType::ReadRequest as u8,
            MsgType::ReadResponse as u8,
            MsgType::PartitionSuffixReadRequest as u8,
            MsgType::RepairWrite as u8,
            MsgType::StreamStart as u8,
            MsgType::StreamChunk as u8,
            MsgType::StreamEnd as u8,
            MsgType::PairWriteForward as u8,
            MsgType::PairWriteAck as u8,
            MsgType::PairCatchUp as u8,
            MsgType::PairCatchUpResponse as u8,
            MsgType::RoleSwap as u8,
            MsgType::PairSchemaSync as u8,
            MsgType::PairDdlForward as u8,
            MsgType::PairDdlAck as u8,
            MsgType::BatchlogWrite as u8,
            MsgType::BatchlogDelete as u8,
            MsgType::BatchlogReplay as u8,
            MsgType::IndexBuildRequest as u8,
            MsgType::IndexBuildComplete as u8,
        ];
        for code in &accord_codes {
            assert!(
                !existing_codes.contains(code),
                "Accord code 0x{code:02x} collides with an existing MsgType"
            );
        }
    }

    #[test]
    fn accord_message_size_bounded() {
        // MAX_MESSAGE_SIZE: the codec's max_frame_body_size governs this.
        // The default in production is 256 MiB. A 2 MiB payload is well within bounds.
        // This test verifies that serialization of a 2 MiB Accord message succeeds.
        let two_mb = vec![0xABu8; 2 * 1024 * 1024];
        let payload = Bytes::from(two_mb);
        let msg = Message::AccordPreAccept(payload.clone());

        let mut buf = BytesMut::new();
        msg.encode(&mut buf)
            .expect("2 MiB payload should encode successfully");

        let decoded = Message::decode(MsgType::AccordPreAccept, &mut buf.freeze())
            .expect("2 MiB payload should decode successfully");
        assert_eq!(decoded, Message::AccordPreAccept(payload));
    }

    #[test]
    fn accord_message_unknown_type_rejected() {
        // MsgType 0xFF is not a valid message type. Attempting to parse it must
        // return an error, not panic.
        let result = MsgType::try_from(0xFF);
        assert!(
            result.is_err(),
            "0xFF should be rejected as unknown MsgType"
        );

        // Also verify the error is specifically UnknownMessageType
        match result {
            Err(crate::error::NetError::UnknownMessageType(code)) => {
                assert_eq!(code, 0xFF);
            }
            other => panic!("expected UnknownMessageType(0xFF), got {other:?}"),
        }
    }

    #[test]
    fn accord_message_type_enum_has_all_variants() {
        // Verify the AccordMessageType enum covers all 11 Accord message types.
        assert_eq!(AccordMessageType::ALL.len(), 11);
    }
}
