//! Every web admin call carries the operator's credentials.
//!
//! 2026-11-06: against an auth-enabled cluster `ferrosa-ctl repair` (and every
//! other web-API command) failed with `401 missing Authorization header`,
//! because none of them sent one. A capture server records the request each
//! command makes.

use super::*;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

/// Accept one request, answer `status` with `body`, return its request line
/// plus headers.
async fn serve_once(status: u16, body: &'static str) -> (u16, tokio::task::JoinHandle<String>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = tokio::io::BufReader::new(socket);
        let mut headers = String::new();
        let mut length = 0;
        loop {
            let mut line = String::new();
            socket.read_line(&mut line).await.unwrap();
            assert!(!line.is_empty(), "client closed before the end of headers");
            if line == "\r\n" {
                break;
            }
            if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                length = v.trim().parse::<usize>().unwrap();
            }
            headers.push_str(&line);
            assert!(headers.len() <= 8192);
        }
        assert!(length <= 8192);
        let mut request = vec![0; length];
        socket.read_exact(&mut request).await.unwrap();
        let response = format!(
            "HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        headers
    });
    (port, server)
}

fn operator() -> crate::credentials::Credentials {
    crate::credentials::Credentials::new("operator", "secret")
}

/// `Basic base64("operator:secret")`.
const OPERATOR_AUTH: &str = "authorization: basic b3blcmf0b3i6c2vjcmv0";

#[tokio::test]
async fn repair_sends_basic_auth_to_the_repair_endpoint() {
    let (port, server) = serve_once(200, r#"{"sessions":0}"#).await;
    repair(
        "127.0.0.1",
        port,
        "agent_memory",
        "temporal_edges",
        3,
        Some(&operator()),
    )
    .await
    .unwrap();
    let headers = server.await.unwrap();
    assert!(
        headers.starts_with(
            "POST /api/cluster/repair?keyspace=agent_memory&table=temporal_edges&rf=3 HTTP/1.1\r\n"
        ),
        "got: {headers}"
    );
    assert!(
        headers.to_ascii_lowercase().contains(OPERATOR_AUTH),
        "got: {headers}"
    );
}

#[tokio::test]
async fn repair_without_credentials_sends_no_authorization_header() {
    let (port, server) = serve_once(200, r#"{"sessions":0}"#).await;
    repair("127.0.0.1", port, "ks", "t", 3, None).await.unwrap();
    let headers = server.await.unwrap();
    assert!(
        !headers.to_ascii_lowercase().contains("authorization:"),
        "got: {headers}"
    );
}

#[tokio::test]
async fn ring_sends_basic_auth() {
    let (port, server) = serve_once(200, "[]").await;
    ring("127.0.0.1", port, Some(&operator())).await.unwrap();
    let headers = server.await.unwrap();
    assert!(
        headers.starts_with("GET /api/cluster/ring HTTP/1.1\r\n"),
        "got: {headers}"
    );
    assert!(
        headers.to_ascii_lowercase().contains(OPERATOR_AUTH),
        "got: {headers}"
    );
}

#[tokio::test]
async fn snapshot_list_sends_basic_auth() {
    let (port, server) = serve_once(200, "[]").await;
    snapshot_list("127.0.0.1", port, Some(&operator()))
        .await
        .unwrap();
    let headers = server.await.unwrap();
    assert!(
        headers.starts_with("GET /api/snapshots HTTP/1.1\r\n"),
        "got: {headers}"
    );
    assert!(
        headers.to_ascii_lowercase().contains(OPERATOR_AUTH),
        "got: {headers}"
    );
}

#[tokio::test]
async fn rebalance_sends_basic_auth() {
    let (port, server) = serve_once(200, "{}").await;
    rebalance("127.0.0.1", port, Some(&operator()))
        .await
        .unwrap();
    let headers = server.await.unwrap();
    assert!(
        headers.to_ascii_lowercase().contains(OPERATOR_AUTH),
        "got: {headers}"
    );
}
