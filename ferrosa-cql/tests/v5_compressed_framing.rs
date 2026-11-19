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

/// The DataStax Java driver's actual setup request — `SELECT cluster_name FROM
/// system.local` on stream 42, protocol v5, LZ4 negotiated, issue #664.
///
/// Two things are load-bearing and neither was covered by the fixtures above:
///
/// * The segment carries `uncompressed length = 0` (the request is too small to
///   be worth compressing), so its payload is the plaintext envelope.
/// * The envelope's flags byte is **0x01**. The driver still sets the deprecated
///   compression bit on every non-STARTUP/OPTIONS envelope at v5, even though
///   §2.4.1.2 of the spec says it is ignored.
///
/// A decoder that honours that bit decompresses plaintext. The body's first four
/// bytes are `0x00000025` — the query-string length, 37 — which LZ4 reads as an
/// uncompressed length, and the decode fails with "the offset to copy is not
/// contained in the decompressed buffer". This fixture is constructed from the
/// driver's own encoding rules (`SegmentCodec.encodeHeader`,
/// `FrameCodec.computeFlags`, `ByteBufCompressor`); only the query text is ours.
const DRIVER_V5_QUERY_ENVELOPE_FLAG_SET: &str =
    "35000000047ce7e20501002a070000002c0000002553454c45435420636c75737465725f6e616d652046524f4d2073797374656d2e6c6f63616c000100090ceb66";

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

#[test]
fn decodes_the_java_driver_setup_query_whose_envelope_flag_is_set() {
    // Issue #664. The driver's control connection fails at "step 3 (QUERY
    // (SELECT cluster_name FROM system.local))" against a v5 connection with
    // LZ4 negotiated. This is that request, byte for byte.
    let mut codec = compressed_v5_codec();
    let mut src = BytesMut::from(&hex_to_bytes(DRIVER_V5_QUERY_ENVELOPE_FLAG_SET)[..]);

    let frame = codec
        .decode(&mut src)
        .expect("the driver's own setup query must decode")
        .expect("frame must be complete");

    assert!(src.is_empty(), "decoder must consume the whole frame");
    assert_eq!(frame.header.stream_id, 42);
    assert_eq!(frame.header.opcode, Opcode::Query);

    // The body must be the plaintext envelope body the driver encoded, read
    // back through the same fields the server parses: [long string][consistency][flags].
    assert_eq!(
        frame.body.len(),
        44,
        "body must be the plaintext bytes, not an attempted LZ4 decode of them"
    );
    let query = b"SELECT cluster_name FROM system.local";
    let mut cursor = &frame.body[..];
    assert_eq!(
        u32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]) as usize,
        query.len()
    );
    assert_eq!(&cursor[4..4 + query.len()], &query[..]);
    cursor = &cursor[4 + query.len()..];
    assert_eq!(&cursor[..2], &[0x00, 0x01], "consistency LOCAL_ONE");
    assert_eq!(cursor[2], 0x00, "query flags: none");
}

#[test]
fn the_envelope_flag_is_the_only_difference_from_a_working_frame() {
    // Control: the same request with the deprecated bit CLEARED, CRC32
    // recomputed so the frame is valid. That shape already worked, which is why
    // this bug survived every fixture captured from the Python driver. The pair
    // proves the flag alone decides whether a v5 body is decoded or destroyed.
    const WITHOUT_FLAG: &str =
        "35000000047ce7e20500002a070000002c0000002553454c45435420636c75737465725f6e616d652046524f4d2073797374656d2e6c6f63616c00010004f0270f";

    let mut codec = compressed_v5_codec();
    let mut src = BytesMut::from(&hex_to_bytes(DRIVER_V5_QUERY_ENVELOPE_FLAG_SET)[..]);
    let flagged = codec.decode(&mut src).expect("flagged").expect("complete");

    let mut codec = compressed_v5_codec();
    let mut src = BytesMut::from(&hex_to_bytes(WITHOUT_FLAG)[..]);
    let plain = codec
        .decode(&mut src)
        .expect("unflagged")
        .expect("complete");

    // The two wires are identical apart from the envelope flags byte (offset 9)
    // and the trailing CRC32, which covers that byte.
    assert_eq!(flagged.header.stream_id, plain.header.stream_id);
    assert_eq!(flagged.header.opcode, plain.header.opcode);
    assert_eq!(
        flagged.body, plain.body,
        "the deprecated 0x01 bit must not change how a v5 body is decoded"
    );
}
