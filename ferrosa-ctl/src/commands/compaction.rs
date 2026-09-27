//! Request node-local operator cancellation through the authenticated admin API.
//! Correctness: report request acceptance, preserve scope, and reject malformed replies.
//! Last revised: 2026-09-27
//! Last changed: Add compaction stop with optional table scope and HTTP Basic auth.

use std::io::{BufRead, Read};
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use super::WebError;

#[derive(Serialize)]
struct StopRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    keyspace: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    table: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
struct StopResponse {
    status: String,
    node_id: String,
    keyspace: Option<String>,
    table: Option<String>,
    matched_tasks: usize,
    already_cancelled_tasks: usize,
}

async fn send_stop(
    addr: SocketAddr,
    keyspace: Option<&str>,
    table: Option<&str>,
    credentials: Option<(&str, &str)>,
) -> Result<StopResponse, WebError> {
    match (keyspace, table) {
        (None, None) => {}
        (Some(keyspace), Some(table))
            if !keyspace.trim().is_empty() && !table.trim().is_empty() => {}
        _ => return Err("supply both non-empty --keyspace and --table, or neither".into()),
    }
    let client = reqwest::Client::builder()
        // Stop only takes a registry snapshot; no compaction completion wait.
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let mut request = client
        .post(format!("http://{addr}/api/compaction/stop"))
        .json(&StopRequest { keyspace, table });
    if let Some((username, password)) = credentials {
        request = request.basic_auth(username, Some(password));
    }
    let mut response = request.send().await?;
    let status = response.status();
    // The acknowledgement is a few scalar fields, never an unbounded log/body.
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > 4096 {
            return Err("compaction stop response exceeds 4096 bytes".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    if status != reqwest::StatusCode::ACCEPTED {
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
        let message = body
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("server did not acknowledge the cancellation request");
        return Err(format!("compaction stop failed (HTTP {status}): {message}").into());
    }
    let report: StopResponse = serde_json::from_slice(&bytes)?;
    if report.status != "cancellation_requested"
        || report.node_id.is_empty()
        || report.already_cancelled_tasks > report.matched_tasks
        || report.keyspace.as_deref() != keyspace
        || report.table.as_deref() != table
    {
        return Err("invalid compaction stop acknowledgement or mismatched scope".into());
    }
    Ok(report)
}

pub async fn stop(
    cql_addr: SocketAddr,
    web_port: u16,
    keyspace: Option<&str>,
    table: Option<&str>,
    username: Option<&str>,
    password_stdin: bool,
) -> Result<(), WebError> {
    let password = if username.is_some() {
        if password_stdin {
            let mut line = String::new();
            // Passwords are one line; bound accidental file/pipe input as well.
            let mut input = std::io::stdin().lock().take(4097);
            input.read_line(&mut line)?;
            if line.len() > 4096 {
                return Err("password input exceeds 4096 bytes".into());
            }
            Some(line.trim_end_matches(['\r', '\n']).to_owned())
        } else {
            Some(crate::auth::prompt_password("Admin password: ")?)
        }
    } else {
        None
    };
    let credentials = username.zip(password.as_deref());
    let report = send_stop(
        SocketAddr::new(cql_addr.ip(), web_port),
        keyspace,
        table,
        credentials,
    )
    .await?;
    println!(
        "Cancellation requested on node {} for {} compaction task(s) ({} already cancelled).",
        report.node_id, report.matched_tasks, report.already_cancelled_tasks
    );
    println!("Committed replacements finish normally; future compactions remain enabled.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

    async fn serve_once(
        status: u16,
        body: String,
    ) -> (SocketAddr, tokio::task::JoinHandle<(String, String)>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio::io::BufReader::new(socket);
            let mut headers = String::new();
            let mut length = 0;
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                assert!(!line.is_empty());
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse::<usize>().unwrap();
                }
                headers.push_str(&line);
                assert!(headers.len() <= 4096);
            }
            assert!(length <= 4096);
            let mut request = vec![0; length];
            socket.read_exact(&mut request).await.unwrap();
            let response = format!("HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            socket.write_all(response.as_bytes()).await.unwrap();
            (headers, String::from_utf8(request).unwrap())
        });
        (address, server)
    }

    fn acknowledgement(keyspace: Option<&str>, table: Option<&str>) -> String {
        serde_json::json!({"status":"cancellation_requested", "node_id":"node-1",
            "keyspace": keyspace, "table": table, "matched_tasks": 2, "already_cancelled_tasks": 1})
        .to_string()
    }

    #[tokio::test]
    async fn compaction_stop_cli_sends_scoped_json_and_authentication() {
        let (address, server) =
            serve_once(202, acknowledgement(Some("ks &quoted"), Some("table/one"))).await;
        let report = send_stop(
            address,
            Some("ks &quoted"),
            Some("table/one"),
            Some(("operator", "secret")),
        )
        .await
        .unwrap();
        assert_eq!(report.matched_tasks, 2);
        let (headers, body) = server.await.unwrap();
        assert!(headers.starts_with("POST /api/compaction/stop HTTP/1.1\r\n"));
        assert!(headers
            .to_ascii_lowercase()
            .contains("authorization: basic b3blcmf0b3i6c2vjcmv0"));
        let body: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({"keyspace":"ks &quoted","table":"table/one"})
        );
    }

    #[tokio::test]
    async fn compaction_stop_cli_sends_node_scope_without_credentials() {
        let (address, server) = serve_once(202, acknowledgement(None, None)).await;
        send_stop(address, None, None, None).await.unwrap();
        let (headers, body) = server.await.unwrap();
        assert!(!headers.to_ascii_lowercase().contains("authorization:"));
        assert_eq!(body, "{}");
    }

    #[tokio::test]
    async fn compaction_stop_cli_rejects_errors_malformed_and_mismatched_responses() {
        for (status, body) in [
            (401, r#"{"error":"authentication failed"}"#.to_owned()),
            (404, r#"{"error":"table not registered"}"#.to_owned()),
            (200, acknowledgement(None, None)),
            (202, "{}".into()),
            (202, acknowledgement(Some("other"), Some("table"))),
            (202, " ".repeat(4097)),
        ] {
            let (address, server) = serve_once(status, body).await;
            assert!(send_stop(address, None, None, None).await.is_err());
            server.await.unwrap();
        }
        let address = "127.0.0.1:1".parse().unwrap();
        assert!(send_stop(address, Some("ks"), None, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("both"));
    }
}
