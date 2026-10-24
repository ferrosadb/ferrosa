//! v5 framing on a connection where compression was negotiated.
//!
//! A compressed v5 connection uses a FIVE-byte segment header holding both the
//! payload length and the uncompressed length, with the CRC24 taken over all
//! five bytes. Reading the three-byte header of an uncompressed connection puts
//! the CRC24 check on bytes the peer never checksummed, so every frame on such a
//! connection fails its header check -- which is what the reference driver
//! reports as `CRC mismatch on header`.
//!
//! The wire fixtures below are **byte-exact output from the Python driver's
//! `SegmentCodec` with LZ4 negotiated**. They are the other half of the
//! contract, so they are captured rather than constructed here.

use bytes::BytesMut;
use ferrosa_cql::frame::{Compression, CqlCodec, CqlFrame, FrameHeader, Opcode};
use tokio_util::codec::{Decoder, Encoder};

/// One compressed frame: payload 20 bytes, uncompressed length 209, self-contained.
const DRIVER_COMPRESSED_COMPRESSIBLE: &str =
    "1400a2010421f7b4af0500000108000000c8410100af504141414141c9fe2fb9";

/// One frame the driver chose NOT to compress: payload 17, uncompressed length 0.
/// The header is still five bytes wide because the connection negotiated LZ4.
const DRIVER_COMPRESSED_NOT_WORTH_COMPRESSING: &str =
    "11000000048e9ea305000002070000000853454c4543542031251f7480";

fn hex_to_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex fixture"))
        .collect()
}

fn compressed_v5_codec() -> CqlCodec {
    let mut codec = CqlCodec::new(64 * 1024 * 1024);
    codec.set_compression(Compression::Lz4);
    codec.enable_v5_framing();
    codec
}

#[test]
fn decodes_a_driver_frame_that_was_compressed() {
    let mut codec = compressed_v5_codec();
    let mut src = BytesMut::from(&hex_to_bytes(DRIVER_COMPRESSED_COMPRESSIBLE)[..]);

    let frame = codec
        .decode(&mut src)
        .expect("compressed v5 frame must decode")
        .expect("frame must be complete");

    assert!(src.is_empty(), "decoder must consume the whole frame");
    assert_eq!(frame.body.len(), 200, "decompressed body length");
    assert!(
        frame.body.iter().all(|&b| b == b'A'),
        "decompressed body must be the original bytes"
    );
    assert_eq!(frame.header.stream_id, 1);
    assert_eq!(frame.header.opcode, Opcode::Result);
}

#[test]
fn decodes_a_driver_frame_that_stayed_uncompressed() {
    // The message is too small for compression to help, so the driver sent it
    // raw -- but on a compressed connection the header is still five bytes.
    let mut codec = compressed_v5_codec();
    let mut src = BytesMut::from(&hex_to_bytes(DRIVER_COMPRESSED_NOT_WORTH_COMPRESSING)[..]);

    let frame = codec
        .decode(&mut src)
        .expect("uncompressed frame on a compressed connection must decode")
        .expect("frame must be complete");

    assert!(src.is_empty());
    assert_eq!(&frame.body[..], b"SELECT 1");
    assert_eq!(frame.header.stream_id, 2);
}

#[test]
fn compressed_framing_is_five_bytes_wide() {
    // The negative control for the width itself: bytes that decode under a
    // compressed codec must NOT decode under an uncompressed one, and vice
    // versa. Without this, a fix that only widened the *reader* would pass the
    // cases above while still emitting narrow frames.
    let wire = hex_to_bytes(DRIVER_COMPRESSED_NOT_WORTH_COMPRESSING);

    let mut wide = compressed_v5_codec();
    let mut src = BytesMut::from(&wire[..]);
    assert!(wide.decode(&mut src).expect("wide decode").is_some());

    let mut narrow = CqlCodec::new(64 * 1024 * 1024);
    narrow.enable_v5_framing();
    let mut src = BytesMut::from(&wire[..]);
    let err = narrow
        .decode(&mut src)
        .expect_err("a three-byte reader must reject a five-byte header");
    let message = format!("{err}");
    assert!(
        message.contains("CRC24"),
        "expected a header CRC failure, got: {message}"
    );
}

#[test]
fn round_trips_a_compressed_message() {
    // Encode with our writer, decode with our reader: the writer must produce
    // the wide header and the reader must undo the compression.
    //
    // This one is a REGRESSION GUARD, not the red that produced the fix: before
    // the fix a narrow writer and narrow reader were self-consistent, so it
    // passed. The driver fixtures above are what fail without the fix.
    let body: Vec<u8> = vec![b'B'; 500];
    let header = FrameHeader {
        version: 0x05,
        flags: 0,
        stream_id: 11,
        opcode: Opcode::Result,
        length: body.len() as u32,
    };
    let frame = CqlFrame {
        header,
        body: body.clone().into(),
    };

    let mut writer = compressed_v5_codec();
    let mut wire = BytesMut::new();
    writer.encode(frame, &mut wire).expect("encode");

    // A wide header: the payload shrank, so total is less than 5+3+509+4.
    assert!(
        wire.len() < 5 + 3 + 509 + 4,
        "compressible body must shrink"
    );

    let mut reader = compressed_v5_codec();
    let decoded = reader
        .decode(&mut wire)
        .expect("decode")
        .expect("complete frame");
    assert!(wire.is_empty());
    assert_eq!(&decoded.body[..], &body[..]);
    assert_eq!(decoded.header.stream_id, 11);
}

#[test]
fn error_frames_on_a_compressed_connection_still_use_the_wide_header() {
    // ERROR frames are deliberately sent uncompressed, but they travel on the
    // same connection and so must still carry the five-byte header.
    let body = vec![0u8; 12];
    let header = FrameHeader {
        version: 0x05,
        flags: 0,
        stream_id: 5,
        opcode: Opcode::Error,
        length: body.len() as u32,
    };
    let frame = CqlFrame {
        header,
        body: body.clone().into(),
    };

    let mut writer = compressed_v5_codec();
    let mut wire = BytesMut::new();
    writer.encode(frame, &mut wire).expect("encode");

    // Exactly wide-header sized: nothing was compressed, so no savings.
    assert_eq!(wire.len(), 5 + 3 + 9 + body.len() + 4);

    let mut reader = compressed_v5_codec();
    let decoded = reader
        .decode(&mut wire)
        .expect("decode")
        .expect("complete frame");
    assert!(wire.is_empty());
    assert_eq!(decoded.header.opcode, Opcode::Error);
    assert_eq!(&decoded.body[..], &body[..]);
}
