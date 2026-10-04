//! The frame decoders read untrusted bytes straight off the socket, before any
//! authentication. Whatever arrives, they return a message, "need more", or an
//! error; they never panic, and a complete frame is consumed exactly.

use bytes::BytesMut;
use ferrosa_postgres::codec::{read_frontend, read_startup};
use ferrosa_postgres::MAX_MESSAGE_LEN;
use proptest::prelude::*;

/// A frame whose length field is consistent with its body, so decoding goes
/// past the length checks into the per-message parsers.
fn tagged_frame() -> impl Strategy<Value = Vec<u8>> {
    (
        proptest::sample::select(b"BCDEHPQSXcdfp".to_vec()),
        proptest::collection::vec(any::<u8>(), 0..128),
    )
        .prop_map(|(tag, body)| {
            let mut frame = vec![tag];
            frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
            frame.extend_from_slice(&body);
            frame
        })
}

fn startup_frame() -> impl Strategy<Value = Vec<u8>> {
    (
        prop_oneof![
            Just(196_608i32),
            Just(80_877_103),
            Just(80_877_102),
            any::<i32>()
        ],
        proptest::collection::vec(any::<u8>(), 0..96),
    )
        .prop_map(|(code, body)| {
            let mut frame = ((body.len() + 8) as i32).to_be_bytes().to_vec();
            frame.extend_from_slice(&code.to_be_bytes());
            frame.extend_from_slice(&body);
            frame
        })
}

proptest! {
    #[test]
    fn frontend_decoding_never_panics(
        bytes in prop_oneof![tagged_frame(), proptest::collection::vec(any::<u8>(), 0..160)],
    ) {
        let mut buf = BytesMut::from(bytes.as_slice());
        // Drain as a connection would: every Ok(Some) must consume bytes, so
        // the loop is bounded by the input length.
        for _ in 0..=bytes.len() {
            let before = buf.len();
            match read_frontend(&mut buf) {
                Ok(Some(_)) => prop_assert!(buf.len() < before, "a message consumed nothing"),
                Ok(None) | Err(_) => break,
            }
        }
    }

    /// A well-formed tagged frame is consumed whole, never more and never less,
    /// so the next message starts at the right byte.
    #[test]
    fn a_decoded_frame_consumes_exactly_its_length(
        frame in tagged_frame(),
        trailer in proptest::collection::vec(any::<u8>(), 0..16),
    ) {
        let mut buf = BytesMut::from(frame.as_slice());
        buf.extend_from_slice(&trailer);
        if let Ok(Some(_)) = read_frontend(&mut buf) {
            prop_assert_eq!(&buf[..], &trailer[..]);
        }
    }

    #[test]
    fn startup_decoding_never_panics(
        bytes in prop_oneof![startup_frame(), proptest::collection::vec(any::<u8>(), 0..128)],
    ) {
        let mut buf = BytesMut::from(bytes.as_slice());
        let _ = read_startup(&mut buf);
    }

    /// A length beyond the limit is refused from the 5-byte header alone; the
    /// decoder never waits to buffer it.
    #[test]
    fn oversized_lengths_are_refused_before_buffering(
        tag in any::<u8>(),
        excess in 1usize..1 << 20,
    ) {
        let len = (MAX_MESSAGE_LEN + excess).min(i32::MAX as usize) as i32;
        let mut buf = BytesMut::from(&[tag][..]);
        buf.extend_from_slice(&len.to_be_bytes());
        prop_assert!(read_frontend(&mut buf).is_err());
    }
}
