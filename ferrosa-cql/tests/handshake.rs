//! Integration test: CQL v5 auth handshake.
//!
//! These tests validate the CQL protocol handshake sequence. They use a 2-second
//! timeout so they fail fast when the connection handler hasn't been implemented yet
//! (Part B), rather than hanging indefinitely.

use std::time::Duration;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use ferrosa_cql::frame::*;
use ferrosa_cql::server::{CqlServer, ServerConfig};

/// Timeout for handshake operations — fail fast if the server doesn't respond.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);

fn encode_startup_frame() -> BytesMut {
    let mut body = BytesMut::new();
    body.put_u16(1);
    let key = b"CQL_VERSION";
    body.put_u16(key.len() as u16);
    body.put_slice(key);
    let val = b"3.0.0";
    body.put_u16(val.len() as u16);
    body.put_slice(val);

    let header = FrameHeader {
        version: VERSION_REQUEST,
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

fn encode_auth_response(username: &str, password: &str) -> BytesMut {
    let sasl = format!("\0{username}\0{password}");
    let sasl_bytes = sasl.as_bytes();

    let mut body = BytesMut::new();
    body.put_i32(sasl_bytes.len() as i32);
    body.put_slice(sasl_bytes);

    let header = FrameHeader {
        version: VERSION_REQUEST,
        flags: 0,
        stream_id: 0,
        opcode: Opcode::AuthResponse,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(&body);
    buf
}

async fn send_startup(stream: &mut TcpStream) {
    let buf = encode_startup_frame();
    stream.write_all(&buf).await.unwrap();
}

struct RawFrame {
    opcode: Opcode,
    body: Vec<u8>,
}

async fn read_frame(stream: &mut TcpStream) -> RawFrame {
    let mut hdr_buf = vec![0u8; HEADER_SIZE];
    timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut hdr_buf))
        .await
        .expect("timed out waiting for frame header — connection handler not implemented?")
        .unwrap();
    let header = FrameHeader::decode(&hdr_buf).unwrap();
    let mut body = vec![0u8; header.length as usize];
    if !body.is_empty() {
        timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut body))
            .await
            .expect("timed out waiting for frame body")
            .unwrap();
    }
    RawFrame {
        opcode: header.opcode,
        body,
    }
}

async fn send_raw_frame(stream: &mut TcpStream, opcode: Opcode, body: &[u8]) {
    let header = FrameHeader {
        version: VERSION_REQUEST,
        flags: 0,
        stream_id: 0,
        opcode,
        length: body.len() as u32,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(body);
    stream.write_all(&buf).await.unwrap();
}

fn test_config(auth_disabled: bool) -> ServerConfig {
    ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        max_connections: 10,
        max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        max_in_flight_per_connection: 128,
        auth_disabled,
    }
}

#[tokio::test]
#[ignore] // TODO(Part B): remove #[ignore] when connection handler is implemented
async fn startup_then_authenticate_then_auth_success() {
    let server = CqlServer::new(test_config(false));
    let addr = server.start_background().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send STARTUP
    let startup = encode_startup_frame();
    stream.write_all(&startup).await.unwrap();

    // Read AUTHENTICATE response
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);

    // Send AUTH_RESPONSE with valid credentials
    let auth = encode_auth_response("cassandra", "cassandra");
    stream.write_all(&auth).await.unwrap();

    // Read AUTH_SUCCESS
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::AuthSuccess);
}

#[tokio::test]
#[ignore] // TODO(Part B): remove #[ignore] when connection handler is implemented
async fn malformed_sasl_payload_returns_bad_credentials() {
    let server = CqlServer::new(test_config(false));
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    send_startup(&mut stream).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);

    let bad_payload = b"not-valid-sasl";
    let mut body = Vec::new();
    body.extend_from_slice(&(bad_payload.len() as i32).to_be_bytes());
    body.extend_from_slice(bad_payload);
    send_raw_frame(&mut stream, Opcode::AuthResponse, &body).await;

    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Error);
    let error_code = i32::from_be_bytes(resp.body[..4].try_into().unwrap());
    assert_eq!(error_code, 0x0100);
}

#[tokio::test]
#[ignore] // TODO(Part B): remove #[ignore] when connection handler is implemented
async fn three_failed_auth_attempts_closes_connection() {
    let server = CqlServer::new(test_config(false));
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    send_startup(&mut stream).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Authenticate);

    let bad_payload = b"not-valid-sasl";
    for _ in 0..3 {
        let mut body = Vec::new();
        body.extend_from_slice(&(bad_payload.len() as i32).to_be_bytes());
        body.extend_from_slice(bad_payload);
        send_raw_frame(&mut stream, Opcode::AuthResponse, &body).await;

        let resp = read_frame(&mut stream).await;
        assert_eq!(resp.opcode, Opcode::Error);
    }

    let mut buf = vec![0u8; 64];
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(n, 0, "connection should be closed after 3 auth failures");
}

#[tokio::test]
#[ignore] // TODO(Part B): remove #[ignore] when connection handler is implemented
async fn auth_disabled_startup_returns_ready() {
    let server = CqlServer::new(test_config(true));
    let addr = server.start_background().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    send_startup(&mut stream).await;
    let resp = read_frame(&mut stream).await;
    assert_eq!(resp.opcode, Opcode::Ready);
}
