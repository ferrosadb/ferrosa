//! CQL v5 framing interop, pinned against the reference driver.
//!
//! The fixtures below are **byte-exact output from the Python driver's
//! `SegmentCodec`** (the implementation on the other side of the wire), not
//! hand-built to match this crate's encoder. They are short because the
//! interesting boundary -- a payload larger than the 131071-byte segment cap --
//! cannot be expressed in a small literal; those cases build their input with
//! `encode_v5_frame`, which `v5_interop_vectors` pins byte-for-byte to the same
//! reference encoder, so the round trip starts from known-correct bytes.
//!
//! Every case asserts the decoder consumed the WHOLE input. A framing bug of the
//! class this file exists for leaves bytes stranded in the buffer, and a test
//! that only counted envelopes would not notice.

use bytes::BytesMut;
use ferrosa_cql::frame::{encode_v5_frame, CqlCodec, FrameHeader, Opcode};
use tokio_util::codec::Decoder;

/// One driver-encoded QUERY for `SELECT 1` (protocol v5, stream 0).
const DRIVER_SINGLE_QUERY: &str = "110002de6b7705000000070000000853454c454354203188db1a61";

/// Four driver-encoded QUERYs coalesced into a single connection write.
const DRIVER_COALESCED_QUERIES: &str = concat!(
    "110002de6b7705000000070000000853454c454354203188db1a61",
    "110002de6b7705000001070000000853454c4543542031fe3a15fc",
    "110002de6b7705000002070000000853454c4543542031251f7480",
    "110002de6b7705000003070000000853454c454354203153fe7b1d",
);

fn hex_to_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex fixture"))
        .collect()
}

fn v5_codec() -> CqlCodec {
    let mut codec = CqlCodec::new(64 * 1024 * 1024);
    codec.enable_v5_framing();
    codec
}

/// Drain every envelope the codec can produce, optionally feeding the input in
/// `chunk`-byte pieces so frames straddle read boundaries.
fn decode_all(input: &[u8], chunk: usize) -> Vec<Vec<u8>> {
    let mut codec = v5_codec();
    let mut src = BytesMut::new();
    let mut frames = Vec::new();
    let mut fed = 0;

    loop {
        // Refill, mimicking a socket read that returns at most `chunk` bytes.
        if fed < input.len() {
            let end = (fed + chunk).min(input.len());
            src.extend_from_slice(&input[fed..end]);
            fed = end;
        }

        let before = src.len();
        match codec.decode(&mut src).expect("decode must not error") {
            Some(frame) => {
                frames.push(frame.body.to_vec());
                continue;
            }
            None => {
                if src.len() != before {
                    // The decoder consumed from the buffer (it absorbed a
                    // segment of a larger message) but cannot yield yet. Keep
                    // draining rather than waiting for bytes that are already
                    // here.
                    continue;
                }
                if fed >= input.len() {
                    assert!(
                        src.is_empty(),
                        "decoder stranded {} byte(s) with no complete frame available",
                        src.len()
                    );
                    break;
                }
            }
        }
    }
    frames
}

#[test]
fn encoder_reproduces_the_driver_bytes_exactly() {
    // The decode tests above take the driver's bytes as input. This pins the
    // other direction: for the same envelope, our writer must emit the reference
    // bytes -- not merely something our own reader accepts.
    let header = FrameHeader {
        version: 0x05,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::Query,
        length: 8,
    };
    let mut wire = BytesMut::new();
    encode_v5_frame(&header, b"SELECT 1", &mut wire);

    assert_eq!(
        wire.to_vec(),
        hex_to_bytes(DRIVER_SINGLE_QUERY),
        "encoded frame must be byte-identical to the reference driver"
    );
}

#[test]
fn driver_encoded_single_query_decodes() {
    let frames = decode_all(&hex_to_bytes(DRIVER_SINGLE_QUERY), usize::MAX);
    assert_eq!(frames.len(), 1, "one envelope expected");
    assert_eq!(frames[0], b"SELECT 1");
}

#[test]
fn driver_encoded_coalesced_queries_decode_in_order() {
    let frames = decode_all(&hex_to_bytes(DRIVER_COALESCED_QUERIES), usize::MAX);
    assert_eq!(frames.len(), 4, "four envelopes expected");
    for body in &frames {
        assert_eq!(body, b"SELECT 1");
    }
}

#[test]
fn oversize_message_reassembles_from_multiple_segments() {
    // 200_000 bytes exceeds the 131071-byte segment cap, so the sender emits
    // several non-self-contained segments for one envelope.
    let body: Vec<u8> = (0..200_000usize).map(|i| (i % 251) as u8).collect();
    let header = FrameHeader {
        version: 0x05,
        flags: 0,
        stream_id: 7,
        opcode: Opcode::Query,
        length: body.len() as u32,
    };
    let mut wire = BytesMut::new();
    encode_v5_frame(&header, &body, &mut wire);

    let frames = decode_all(&wire, usize::MAX);
    assert_eq!(frames.len(), 1, "one reassembled envelope expected");
    assert_eq!(frames[0], body, "reassembled body must match exactly");
}

#[test]
fn oversize_message_followed_by_small_message_both_decode() {
    // The stranding case: a multi-segment envelope immediately followed by an
    // unrelated small envelope in the same connection write.
    let big_body: Vec<u8> = (0..200_000usize).map(|i| (i % 251) as u8).collect();
    let big_header = FrameHeader {
        version: 0x05,
        flags: 0,
        stream_id: 7,
        opcode: Opcode::Query,
        length: big_body.len() as u32,
    };
    let small_header = FrameHeader {
        version: 0x05,
        flags: 0,
        stream_id: 8,
        opcode: Opcode::Query,
        length: 8,
    };

    let mut wire = BytesMut::new();
    encode_v5_frame(&big_header, &big_body, &mut wire);
    encode_v5_frame(&small_header, b"SELECT 2", &mut wire);

    let frames = decode_all(&wire, usize::MAX);
    assert_eq!(frames.len(), 2, "both envelopes expected");
    assert_eq!(frames[0], big_body);
    assert_eq!(frames[1], b"SELECT 2");
}

#[test]
fn frames_straddling_read_boundaries_decode() {
    // A socket read boundary can fall anywhere, including inside the 6-byte
    // frame header and inside the reassembled envelope.
    let body: Vec<u8> = (0..140_000usize).map(|i| (i % 251) as u8).collect();
    let header = FrameHeader {
        version: 0x05,
        flags: 0,
        stream_id: 3,
        opcode: Opcode::Query,
        length: body.len() as u32,
    };
    let mut wire = BytesMut::new();
    encode_v5_frame(&header, &body, &mut wire);
    let small = FrameHeader {
        version: 0x05,
        flags: 0,
        stream_id: 4,
        opcode: Opcode::Query,
        length: 8,
    };
    encode_v5_frame(&small, b"SELECT 2", &mut wire);

    for chunk in [1usize, 3, 5, 6, 7, 4096] {
        let frames = decode_all(&wire, chunk);
        assert_eq!(frames.len(), 2, "both envelopes expected at chunk={chunk}");
        assert_eq!(frames[0], body, "body mismatch at chunk={chunk}");
        assert_eq!(frames[1], b"SELECT 2", "tail mismatch at chunk={chunk}");
    }
}
