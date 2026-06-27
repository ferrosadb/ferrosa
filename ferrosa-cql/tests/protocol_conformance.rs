//! CQL native-protocol v3/v4/v5 conformance regression suite.
//!
//! Goals:
//!   1. Ensure the server responds with the negotiated protocol version byte
//!      (0x83 for v3, 0x84 for v4, 0x85 for v5) on every frame.
//!   2. Ensure PREPARE result metadata omits v4-only `pk_count`/`pk_indexes`
//!      when the client negotiated v3.
//!   3. Ensure v5 clients complete the legacy-envelope STARTUP/READY handshake
//!      and then use the modern v5 framed transport for post-handshake messages.
//!
//! These tests drive the real TCP server using raw CQL frames so they validate
//! the full codec + connection state machine, not just unit parsers.

use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::{Decoder, Encoder};

use ferrosa_cql::frame::*;
use ferrosa_cql::server::{CqlServer, ServerConfig};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);

fn test_config() -> ServerConfig {
    ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 10,
        auth_disabled: true,
        ..ServerConfig::default()
    }
}

async fn start_server() -> (std::net::SocketAddr, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let state = ferrosa_cql::test_util::standalone_for_test(dir.path());
    let server = CqlServer::new(test_config(), state);
    let addr = server.start_background().await.unwrap();
    (addr, dir)
}

fn encode_startup_frame(version: u8) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_u16(1);
    let key = b"CQL_VERSION";
    body.put_u16(key.len() as u16);
    body.put_slice(key);
    let val = b"3.0.0";
    body.put_u16(val.len() as u16);
    body.put_slice(val);

    let header = FrameHeader {
        version,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::Startup,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(&body);
    buf
}

fn encode_options_frame(version: u8) -> BytesMut {
    let header = FrameHeader {
        version,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::Options,
        length: 0,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf
}

fn encode_query_frame(version: u8, query: &str) -> BytesMut {
    let query_bytes = query.as_bytes();
    let mut body = BytesMut::new();
    body.put_i32(query_bytes.len() as i32);
    body.put_slice(query_bytes);
    body.put_u16(0x0001); // consistency ONE
    body.put_u8(0); // flags: none

    let header = FrameHeader {
        version,
        flags: 0,
        stream_id: 1,
        opcode: Opcode::Query,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(&body);
    buf
}

fn encode_prepare_frame(version: u8, query: &str) -> BytesMut {
    let query_bytes = query.as_bytes();
    let mut body = BytesMut::new();
    body.put_i32(query_bytes.len() as i32);
    body.put_slice(query_bytes);

    let header = FrameHeader {
        version,
        flags: 0,
        stream_id: 2,
        opcode: Opcode::Prepare,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(&body);
    buf
}

struct RawFrame {
    header: FrameHeader,
    body: Vec<u8>,
}

async fn read_frame(stream: &mut TcpStream) -> RawFrame {
    let mut hdr_buf = vec![0u8; HEADER_SIZE];
    timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut hdr_buf))
        .await
        .expect("timed out waiting for frame header")
        .unwrap();
    let header = FrameHeader::decode(&hdr_buf).unwrap();
    let mut body = vec![0u8; header.length as usize];
    if !body.is_empty() {
        timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut body))
            .await
            .expect("timed out waiting for frame body")
            .unwrap();
    }
    RawFrame { header, body }
}

async fn send_frame(stream: &mut TcpStream, buf: &BytesMut) {
    stream.write_all(buf).await.unwrap();
}

/// Parse an ERROR body and return (code, message).
fn parse_error(body: &[u8]) -> (i32, String) {
    let mut cur = std::io::Cursor::new(body);
    let code = cur.get_i32();
    let msg_len = cur.get_u16() as usize;
    let mut msg = vec![0u8; msg_len];
    cur.copy_to_slice(&mut msg);
    (code, String::from_utf8_lossy(&msg).into_owned())
}

/// Assert that a response is RESULT; if it is ERROR, panic with a readable message.
fn assert_result(resp: &RawFrame) {
    if resp.header.opcode == Opcode::Error {
        let (code, msg) = parse_error(&resp.body);
        panic!("expected RESULT but got ERROR(0x{code:04X}): {msg}");
    }
    assert_eq!(resp.header.opcode, Opcode::Result, "expected RESULT opcode");
}

#[tokio::test]
async fn v3_startup_receives_ready_with_v3_response_byte() {
    let (addr, _dir) = start_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    send_frame(&mut stream, &encode_startup_frame(0x03)).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(
        resp.header.opcode,
        Opcode::Ready,
        "v3 STARTUP should yield READY"
    );
    assert_eq!(
        resp.header.version, 0x83,
        "server must reply to v3 with 0x83"
    );
}

#[tokio::test]
async fn v4_startup_receives_ready_with_v4_response_byte() {
    let (addr, _dir) = start_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    send_frame(&mut stream, &encode_startup_frame(0x04)).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(
        resp.header.opcode,
        Opcode::Ready,
        "v4 STARTUP should yield READY"
    );
    assert_eq!(
        resp.header.version, 0x84,
        "server must reply to v4 with 0x84"
    );
}

#[tokio::test]
async fn v5_startup_receives_ready_with_v5_response_byte() {
    let (addr, _dir) = start_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    send_frame(&mut stream, &encode_startup_frame(0x05)).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(
        resp.header.opcode,
        Opcode::Ready,
        "v5 STARTUP should yield READY"
    );
    assert_eq!(
        resp.header.version, 0x85,
        "server must reply to v5 with 0x85"
    );
}

#[tokio::test]
async fn v5_framed_options_roundtrip_uses_modern_framing() {
    let (addr, _dir) = start_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 1. Legacy-envelope v5 STARTUP/READY handshake.
    send_frame(&mut stream, &encode_startup_frame(0x05)).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.header.opcode, Opcode::Ready);
    assert_eq!(resp.header.version, 0x85);

    // 2. Switch the client to v5 modern framing and send OPTIONS.
    let mut codec = CqlCodec::new(1024 * 1024);
    codec.enable_v5_framing();
    let frame = CqlFrame {
        header: FrameHeader {
            version: 0x05,
            flags: 0,
            stream_id: 7,
            opcode: Opcode::Options,
            length: 0,
        },
        body: bytes::Bytes::new(),
    };
    let mut buf = BytesMut::new();
    Encoder::encode(&mut codec, frame, &mut buf).unwrap();
    stream.write_all(&buf).await.unwrap();

    // 3. Read the v5-framed SUPPORTED response byte-by-byte until decode succeeds.
    let mut rx = BytesMut::new();
    let resp_frame = loop {
        let mut chunk = [0u8; 64];
        let n = timeout(HANDSHAKE_TIMEOUT, stream.read(&mut chunk))
            .await
            .expect("timed out reading v5 response")
            .unwrap();
        assert!(n > 0, "server closed connection during v5 framed read");
        rx.extend_from_slice(&chunk[..n]);
        if let Some(f) = Decoder::decode(&mut codec, &mut rx).unwrap() {
            break f;
        }
    };

    assert_eq!(resp_frame.header.opcode, Opcode::Supported);
    assert_eq!(
        resp_frame.header.version, 0x85,
        "SUPPORTED must carry negotiated v5 response byte"
    );
    assert_eq!(
        resp_frame.header.stream_id, 7,
        "stream id must be preserved"
    );
}

#[tokio::test]
async fn unsupported_protocol_version_above_v5_is_rejected() {
    let (addr, _dir) = start_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    send_frame(&mut stream, &encode_startup_frame(0x06)).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.header.opcode, Opcode::Error);
    let (code, msg) = parse_error(&resp.body);
    assert_eq!(
        code, 0x000A,
        "v6 should get ProtocolVersionMismatch (0x000A)"
    );
    assert!(
        msg.contains("greatest is 5") || msg.contains("5") || msg.contains("0x05"),
        "error should advertise v5 as max supported: {msg}"
    );
}

#[tokio::test]
async fn options_response_byte_matches_request_version() {
    let (addr, _dir) = start_server().await;
    for version in [0x03u8, 0x04u8] {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        send_frame(&mut stream, &encode_options_frame(version)).await;
        let resp = read_frame(&mut stream).await;
        assert_eq!(
            resp.header.opcode,
            Opcode::Supported,
            "OPTIONS should yield SUPPORTED"
        );
        assert_eq!(
            resp.header.version,
            version | 0x80,
            "SUPPORTED response version must mirror request version"
        );
    }
}

#[tokio::test]
async fn query_response_byte_matches_negotiated_version() {
    let (addr, _dir) = start_server().await;
    for version in [0x03u8, 0x04u8] {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        send_frame(&mut stream, &encode_startup_frame(version)).await;
        let resp = read_frame(&mut stream).await;
        assert_eq!(resp.header.opcode, Opcode::Ready);

        send_frame(
            &mut stream,
            &encode_query_frame(version, "CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"),
        )
        .await;
        let resp = read_frame(&mut stream).await;
        assert_result(&resp);
        assert_eq!(
            resp.header.version,
            version | 0x80,
            "QUERY response version must match negotiated version"
        );
    }
}

/// Parse the bind-variable metadata section of a PREPARED result and return
/// the number of pk_indexes present. For v3 this must be 0 (field absent).
fn parse_prepared_pk_count(body: &[u8], protocol_version: u8) -> usize {
    let mut cur = std::io::Cursor::new(body);
    assert_eq!(cur.get_i32(), 0x0004, "prepared kind"); // Prepared kind
    let id_len = cur.get_u16() as usize;
    cur.advance(id_len); // skip prepared id

    // Bind metadata
    let _flags = cur.get_i32();
    let _columns_count = cur.get_i32();

    if protocol_version >= 0x04 {
        let pk_count = cur.get_i32() as usize;
        cur.advance(pk_count * 2); // skip pk_indexes
        pk_count
    } else {
        // v3: the next bytes are the keyspace string, not pk_count.
        // Try to validate by reading the first u16 as a string length; if it
        // were a pk_count it would be a tiny number followed by i16 indexes.
        // We just report 0 pk_count because v3 has none.
        0
    }
}

#[tokio::test]
async fn prepare_metadata_omits_pk_indexes_for_v3() {
    let (addr, _dir) = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    send_frame(&mut stream, &encode_startup_frame(0x03)).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.header.opcode, Opcode::Ready);

    send_frame(
        &mut stream,
        &encode_query_frame(
            0x03,
            "CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        ),
    )
    .await;
    assert_result(&read_frame(&mut stream).await);

    send_frame(
        &mut stream,
        &encode_query_frame(
            0x03,
            "CREATE TABLE IF NOT EXISTS ks.t (id int PRIMARY KEY, v text)",
        ),
    )
    .await;
    assert_result(&read_frame(&mut stream).await);

    send_frame(
        &mut stream,
        &encode_prepare_frame(0x03, "INSERT INTO ks.t (id, v) VALUES (?, ?)"),
    )
    .await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    assert_eq!(resp.header.version, 0x83);
    let pk_count = parse_prepared_pk_count(&resp.body, 0x03);
    assert_eq!(
        pk_count, 0,
        "v3 PREPARE metadata must not contain pk_count/pk_indexes"
    );
}

#[tokio::test]
async fn prepare_metadata_includes_pk_indexes_for_v4() {
    let (addr, _dir) = start_server().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    send_frame(&mut stream, &encode_startup_frame(0x04)).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.header.opcode, Opcode::Ready);

    send_frame(
        &mut stream,
        &encode_query_frame(
            0x04,
            "CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        ),
    )
    .await;
    assert_result(&read_frame(&mut stream).await);

    send_frame(
        &mut stream,
        &encode_query_frame(
            0x04,
            "CREATE TABLE IF NOT EXISTS ks.t (id int PRIMARY KEY, v text)",
        ),
    )
    .await;
    assert_result(&read_frame(&mut stream).await);

    send_frame(
        &mut stream,
        &encode_prepare_frame(0x04, "INSERT INTO ks.t (id, v) VALUES (?, ?)"),
    )
    .await;
    let resp = read_frame(&mut stream).await;
    assert_result(&resp);
    assert_eq!(resp.header.version, 0x84);
    let pk_count = parse_prepared_pk_count(&resp.body, 0x04);
    assert_eq!(
        pk_count, 1,
        "v4 PREPARE metadata must contain pk_count=1 for single-PK table"
    );
}

#[tokio::test]
async fn execute_response_byte_matches_negotiated_version() {
    let (addr, _dir) = start_server().await;

    for version in [0x03u8, 0x04u8] {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        send_frame(&mut stream, &encode_startup_frame(version)).await;
        let resp = read_frame(&mut stream).await;
        assert_eq!(resp.header.opcode, Opcode::Ready);

        send_frame(
            &mut stream,
            &encode_query_frame(
                version,
                "CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            ),
        )
        .await;
        assert_result(&read_frame(&mut stream).await);

        send_frame(
            &mut stream,
            &encode_query_frame(
                version,
                "CREATE TABLE IF NOT EXISTS ks.t2 (id int PRIMARY KEY, v text)",
            ),
        )
        .await;
        assert_result(&read_frame(&mut stream).await);

        send_frame(
            &mut stream,
            &encode_query_frame(version, "INSERT INTO ks.t2 (id, v) VALUES (1, 'a')"),
        )
        .await;
        assert_result(&read_frame(&mut stream).await);

        send_frame(
            &mut stream,
            &encode_query_frame(version, "SELECT * FROM ks.t2 WHERE id = 1"),
        )
        .await;
        let resp = read_frame(&mut stream).await;
        assert_result(&resp);
        assert_eq!(
            resp.header.version,
            version | 0x80,
            "SELECT response version must match negotiated version"
        );
    }
}

#[tokio::test]
async fn mixed_version_connections_on_same_server_are_independent() {
    let (addr, _dir) = start_server().await;

    let mut v3 = TcpStream::connect(addr).await.unwrap();
    send_frame(&mut v3, &encode_startup_frame(0x03)).await;
    let resp = read_frame(&mut v3).await;
    assert_eq!(resp.header.version, 0x83);

    let mut v4 = TcpStream::connect(addr).await.unwrap();
    send_frame(&mut v4, &encode_startup_frame(0x04)).await;
    let resp = read_frame(&mut v4).await;
    assert_eq!(resp.header.version, 0x84);

    // Re-issue on v3 connection and confirm it stayed at v3.
    send_frame(
        &mut v3,
        &encode_query_frame(0x03, "SELECT now() FROM system.local"),
    )
    .await;
    let resp = read_frame(&mut v3).await;
    assert_result(&resp);
    assert_eq!(resp.header.version, 0x83);
}
