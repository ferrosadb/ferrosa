//! Bolt v5 TCP server for Neo4j driver compatibility.
//!
//! Accepts connections on a configurable port (default 7687), performs the Bolt
//! handshake, authenticates via the schema, and dispatches Cypher queries to the
//! [`GraphEngine`].

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::Schema;

use crate::bolt::codec::{self, ChunkDecoder, PackValue};
use crate::bolt::handshake::{negotiate_version, rejection_response, version_response, BOLT_MAGIC};
use crate::bolt::message::BoltMessage;
use crate::engine::GraphEngine;
use crate::error::GraphError;
use crate::executor::result::GraphResult;

/// Configuration for the Bolt server.
#[derive(Debug, Clone)]
pub struct BoltConfig {
    /// Address to bind to (default: 0.0.0.0:7687).
    pub bind_addr: SocketAddr,
    /// Maximum message size in bytes.
    pub max_message_size: usize,
    /// Maximum concurrent connections.
    pub max_connections: usize,
}

impl Default for BoltConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:7687".parse().unwrap(),
            max_message_size: 16 * 1024 * 1024, // 16MB
            max_connections: 256,
        }
    }
}

/// Mutable state tracked per connection.
struct ConnectionState {
    /// Whether the client has authenticated.
    authenticated: bool,
    /// Current keyspace / graph database.
    keyspace: String,
    /// Pending result from the last RUN.
    pending_result: Option<GraphResult>,
    /// Auth context for permission checks (set after HELLO/LOGON).
    auth_context: Option<AuthContext>,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            authenticated: false,
            keyspace: String::new(),
            pending_result: None,
            auth_context: None,
        }
    }
}

/// Start the Bolt TCP server.
///
/// Binds a `TcpListener` on `config.bind_addr` and accepts connections in a
/// loop. Each connection is handed to a spawned tokio task. The server respects
/// the `shutdown` watch channel and stops accepting new connections when the
/// signal fires. Active connections are dropped when the spawned tasks complete.
pub async fn start_bolt_server(
    engine: Arc<GraphEngine>,
    schema: Arc<Schema>,
    config: BoltConfig,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(config.bind_addr).await?;
    tracing::info!(addr = %config.bind_addr, "Bolt server listening");

    let active_connections = Arc::new(AtomicUsize::new(0));

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, peer_addr) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::warn!(%e, "failed to accept Bolt connection");
                        continue;
                    }
                };

                let current = active_connections.load(Ordering::Relaxed);
                if current >= config.max_connections {
                    tracing::warn!(
                        %peer_addr,
                        current_connections = current,
                        max = config.max_connections,
                        "rejecting Bolt connection: at capacity"
                    );
                    // Drop the stream — the client sees a connection reset.
                    drop(stream);
                    continue;
                }

                active_connections.fetch_add(1, Ordering::Relaxed);
                let counter = active_connections.clone();
                let engine = engine.clone();
                let schema = schema.clone();
                let cfg = config.clone();

                tokio::spawn(async move {
                    tracing::debug!(%peer_addr, "Bolt connection accepted");
                    if let Err(e) = handle_connection(stream, engine, schema, cfg).await {
                        tracing::debug!(%peer_addr, %e, "Bolt connection ended with error");
                    }
                    counter.fetch_sub(1, Ordering::Relaxed);
                    tracing::debug!(%peer_addr, "Bolt connection closed");
                });
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!("Bolt server shutting down");
                    break;
                }
            }
        }
    }

    Ok(())
}

/// Handle a single Bolt connection from handshake through message loop.
async fn handle_connection(
    mut stream: TcpStream,
    engine: Arc<GraphEngine>,
    schema: Arc<Schema>,
    config: BoltConfig,
) -> Result<(), GraphError> {
    // ── 1. Handshake ───────────────────────────────────────────────────
    let mut handshake_buf = [0u8; 20];
    stream
        .read_exact(&mut handshake_buf)
        .await
        .map_err(|e| GraphError::Internal(format!("handshake read: {e}")))?;

    // Verify magic preamble.
    let magic = &handshake_buf[..4];
    if magic != BOLT_MAGIC {
        tracing::debug!(?magic, "invalid Bolt magic preamble");
        return Err(GraphError::Internal("invalid Bolt magic preamble".into()));
    }

    // Negotiate protocol version from the 4 proposals (bytes 4..20).
    match negotiate_version(&handshake_buf) {
        Some(version) => {
            let resp = version_response(version);
            stream
                .write_all(&resp)
                .await
                .map_err(|e| GraphError::Internal(format!("handshake write: {e}")))?;
        }
        None => {
            let resp = rejection_response();
            let _ = stream.write_all(&resp).await;
            return Err(GraphError::Internal("no supported Bolt version".into()));
        }
    }

    // ── 2. HELLO / authentication ──────────────────────────────────────
    let mut state = ConnectionState::new();
    let mut decoder = ChunkDecoder::new();

    let hello_data = read_message(&mut stream, &mut decoder, config.max_message_size).await?;
    let hello = BoltMessage::decode(&hello_data)
        .map_err(|e| GraphError::Internal(format!("HELLO decode: {e}")))?;

    match hello {
        BoltMessage::Hello { extra } => {
            // Extract credentials from the HELLO fields.
            let username =
                find_string_field(&extra, "principal").unwrap_or_else(|| "cassandra".to_string());
            let password = find_string_field(&extra, "credentials").unwrap_or_default();

            // Extract optional keyspace / database selection.
            if let Some(db) = find_string_field(&extra, "db") {
                state.keyspace = db;
            }

            match schema.authenticate(&username, &password) {
                Ok(auth_ctx) => {
                    state.authenticated = true;
                    state.auth_context = Some(auth_ctx);

                    let success = BoltMessage::Success {
                        metadata: vec![
                            ("server".into(), PackValue::String("Ferrosa/0.1.0".into())),
                            ("connection_id".into(), PackValue::String("bolt-1".into())),
                        ],
                    };
                    send_message(&mut stream, &success).await?;
                }
                Err(_) => {
                    let failure = BoltMessage::Failure {
                        metadata: vec![
                            (
                                "code".into(),
                                PackValue::String("Neo.ClientError.Security.Unauthorized".into()),
                            ),
                            (
                                "message".into(),
                                PackValue::String("authentication failed".into()),
                            ),
                        ],
                    };
                    send_message(&mut stream, &failure).await?;
                    return Err(GraphError::PermissionDenied(
                        "authentication failed".to_string(),
                    ));
                }
            }
        }
        _ => {
            return Err(GraphError::Internal(
                "expected HELLO as first message".into(),
            ));
        }
    }

    // Handle optional LOGON message (Bolt 5+).
    let logon_data = read_message(&mut stream, &mut decoder, config.max_message_size).await?;
    let logon_msg = BoltMessage::decode(&logon_data)
        .map_err(|e| GraphError::Internal(format!("LOGON decode: {e}")))?;

    match logon_msg {
        BoltMessage::Logon { .. } => {
            // Accept LOGON, already authenticated via HELLO.
            let success = BoltMessage::Success { metadata: vec![] };
            send_message(&mut stream, &success).await?;
        }
        // If it's not a LOGON, process it as a regular message.
        other => {
            let reply = process_message(other, &engine, &schema, &mut state).await?;
            send_replies(&mut stream, &reply).await?;
        }
    }

    // ── 3. Message loop ────────────────────────────────────────────────
    loop {
        let msg_data = match read_message(&mut stream, &mut decoder, config.max_message_size).await
        {
            Ok(data) => data,
            Err(GraphError::Internal(msg)) if msg.contains("eof") || msg.contains("EOF") => {
                // Client closed the connection.
                break;
            }
            Err(e) => return Err(e),
        };

        let msg = match BoltMessage::decode(&msg_data) {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!(%e, "failed to decode Bolt message");
                let failure = BoltMessage::Failure {
                    metadata: vec![
                        (
                            "code".into(),
                            PackValue::String("Neo.ClientError.Request.Invalid".into()),
                        ),
                        (
                            "message".into(),
                            PackValue::String(format!("message decode error: {e}")),
                        ),
                    ],
                };
                send_message(&mut stream, &failure).await?;
                continue;
            }
        };

        match msg {
            BoltMessage::Goodbye => {
                tracing::debug!("client sent GOODBYE");
                break;
            }
            other => {
                let replies = process_message(other, &engine, &schema, &mut state).await?;
                send_replies(&mut stream, &replies).await?;
            }
        }
    }

    Ok(())
}

/// Process a single message and produce response messages.
async fn process_message(
    msg: BoltMessage,
    engine: &Arc<GraphEngine>,
    _schema: &Arc<Schema>,
    state: &mut ConnectionState,
) -> Result<Vec<BoltMessage>, GraphError> {
    match msg {
        BoltMessage::Run {
            query,
            params,
            extra,
        } => {
            let auth = state.auth_context.as_ref().cloned().unwrap_or(AuthContext {
                role: "anonymous".to_string(),
                is_superuser: false,
                must_change_password: false,
            });

            // Allow override of keyspace via extra params.
            let keyspace = find_string_field(&extra, "db")
                .or_else(|| find_string_field(&params, "db"))
                .unwrap_or_else(|| state.keyspace.clone());

            match engine.execute(&query, &keyspace, &auth).await {
                Ok(result) => {
                    let fields: Vec<PackValue> = result
                        .columns
                        .iter()
                        .map(|c| PackValue::String(c.clone()))
                        .collect();
                    let success = BoltMessage::Success {
                        metadata: vec![
                            ("fields".into(), PackValue::List(fields)),
                            (
                                "t_first".into(),
                                PackValue::Int(result.stats.execution_ms as i64),
                            ),
                            ("qid".into(), PackValue::Int(0)),
                        ],
                    };
                    state.pending_result = Some(result);
                    Ok(vec![success])
                }
                Err(e) => {
                    let failure = BoltMessage::Failure {
                        metadata: vec![
                            ("code".into(), PackValue::String(error_code(&e).into())),
                            ("message".into(), PackValue::String(e.to_string())),
                        ],
                    };
                    Ok(vec![failure])
                }
            }
        }

        BoltMessage::Pull { .. } => {
            if let Some(result) = state.pending_result.take() {
                let mut replies = Vec::new();

                // Send a RECORD for each row.
                for row in &result.rows {
                    let values: Vec<PackValue> = row.iter().map(json_to_pack_value).collect();
                    replies.push(BoltMessage::Record { values });
                }

                // Send summary SUCCESS.
                let success = BoltMessage::Success {
                    metadata: vec![
                        ("type".into(), PackValue::String("r".into())),
                        (
                            "t_last".into(),
                            PackValue::Int(result.stats.execution_ms as i64),
                        ),
                        (
                            "stats".into(),
                            PackValue::Map(vec![
                                (
                                    "vertices_read".into(),
                                    PackValue::Int(result.stats.vertices_read as i64),
                                ),
                                (
                                    "edges_read".into(),
                                    PackValue::Int(result.stats.edges_read as i64),
                                ),
                                (
                                    "vertices_written".into(),
                                    PackValue::Int(result.stats.vertices_written as i64),
                                ),
                                (
                                    "vertices_deleted".into(),
                                    PackValue::Int(result.stats.vertices_deleted as i64),
                                ),
                            ]),
                        ),
                        ("has_more".into(), PackValue::Bool(false)),
                    ],
                };
                replies.push(success);
                Ok(replies)
            } else {
                // No pending result — send SUCCESS with has_more=false.
                Ok(vec![BoltMessage::Success {
                    metadata: vec![("has_more".into(), PackValue::Bool(false))],
                }])
            }
        }

        BoltMessage::Discard { .. } => {
            state.pending_result = None;
            Ok(vec![BoltMessage::Success { metadata: vec![] }])
        }

        BoltMessage::Reset => {
            state.pending_result = None;
            // Keep auth context — RESET doesn't log out.
            Ok(vec![BoltMessage::Success { metadata: vec![] }])
        }

        BoltMessage::Goodbye => {
            // Handled in caller, but just in case.
            Ok(vec![])
        }

        // HELLO arriving after handshake — protocol error.
        BoltMessage::Hello { .. } | BoltMessage::Logon { .. } | BoltMessage::Logoff => {
            Ok(vec![BoltMessage::Failure {
                metadata: vec![
                    (
                        "code".into(),
                        PackValue::String("Neo.ClientError.Request.Invalid".into()),
                    ),
                    (
                        "message".into(),
                        PackValue::String("unexpected HELLO/LOGON after handshake".into()),
                    ),
                ],
            }])
        }

        // Server-to-client messages should never be received from the client.
        BoltMessage::Success { .. }
        | BoltMessage::Failure { .. }
        | BoltMessage::Record { .. }
        | BoltMessage::Ignored => Ok(vec![BoltMessage::Failure {
            metadata: vec![
                (
                    "code".into(),
                    PackValue::String("Neo.ClientError.Request.Invalid".into()),
                ),
                (
                    "message".into(),
                    PackValue::String("unexpected server message from client".into()),
                ),
            ],
        }]),
    }
}

/// Read a single chunked Bolt message from the stream.
async fn read_message(
    stream: &mut TcpStream,
    _decoder: &mut ChunkDecoder,
    max_message_size: usize,
) -> Result<Vec<u8>, GraphError> {
    let mut message = Vec::new();
    loop {
        // Read chunk header (2 bytes big-endian length).
        let mut hdr = [0u8; 2];
        stream
            .read_exact(&mut hdr)
            .await
            .map_err(|e| GraphError::Internal(format!("read chunk header eof: {e}")))?;
        let chunk_len = u16::from_be_bytes(hdr) as usize;

        if chunk_len == 0 {
            // End-of-message marker.
            return Ok(message);
        }

        if message.len() + chunk_len > max_message_size {
            return Err(GraphError::Internal(format!(
                "message exceeds max size: {} > {max_message_size}",
                message.len() + chunk_len
            )));
        }

        let mut chunk = vec![0u8; chunk_len];
        stream
            .read_exact(&mut chunk)
            .await
            .map_err(|e| GraphError::Internal(format!("read chunk body: {e}")))?;

        message.extend_from_slice(&chunk);
    }
}

/// Encode and write a Bolt message to the stream using chunked framing.
async fn send_message(stream: &mut TcpStream, msg: &BoltMessage) -> Result<(), GraphError> {
    let encoded = msg.encode();
    let chunks = codec::chunk_encode(&encoded, codec::DEFAULT_MAX_CHUNK_SIZE);
    stream
        .write_all(&chunks)
        .await
        .map_err(|e| GraphError::Internal(format!("write message: {e}")))?;
    stream
        .flush()
        .await
        .map_err(|e| GraphError::Internal(format!("flush: {e}")))?;
    Ok(())
}

/// Send multiple reply messages.
async fn send_replies(stream: &mut TcpStream, messages: &[BoltMessage]) -> Result<(), GraphError> {
    for msg in messages {
        send_message(stream, msg).await?;
    }
    Ok(())
}

/// Extract a string value from a PackValue map by key.
fn find_string_field(map: &[(String, PackValue)], key: &str) -> Option<String> {
    map.iter().find_map(|(k, v)| {
        if k == key {
            match v {
                PackValue::String(s) => Some(s.clone()),
                _ => None,
            }
        } else {
            None
        }
    })
}

/// Convert a `serde_json::Value` to a `PackValue`.
fn json_to_pack_value(v: &serde_json::Value) -> PackValue {
    match v {
        serde_json::Value::Null => PackValue::Null,
        serde_json::Value::Bool(b) => PackValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PackValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                PackValue::Float(f)
            } else {
                PackValue::Null
            }
        }
        serde_json::Value::String(s) => PackValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            PackValue::List(arr.iter().map(json_to_pack_value).collect())
        }
        serde_json::Value::Object(obj) => PackValue::Map(
            obj.iter()
                .map(|(k, v)| (k.clone(), json_to_pack_value(v)))
                .collect(),
        ),
    }
}

/// Map a `GraphError` to a Neo4j-style error code string.
fn error_code(err: &GraphError) -> &'static str {
    match err {
        GraphError::Parse(_) => "Neo.ClientError.Statement.SyntaxError",
        GraphError::Validation(_) => "Neo.ClientError.Statement.SemanticError",
        GraphError::PermissionDenied(_) => "Neo.ClientError.Security.Forbidden",
        GraphError::ResourceLimit(_) => "Neo.TransientError.General.OutOfMemoryError",
        GraphError::Timeout => "Neo.TransientError.Transaction.LockClientStopped",
        GraphError::Storage(_) | GraphError::Schema(_) | GraphError::Internal(_) => {
            "Neo.DatabaseError.General.UnknownError"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bolt_config_default() {
        let config = BoltConfig::default();
        assert_eq!(
            config.bind_addr,
            "0.0.0.0:7687".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(config.max_message_size, 16 * 1024 * 1024);
        assert_eq!(config.max_connections, 256);
    }

    #[test]
    fn connection_state_initial() {
        let state = ConnectionState::new();
        assert!(!state.authenticated);
        assert!(state.keyspace.is_empty());
        assert!(state.pending_result.is_none());
        assert!(state.auth_context.is_none());
    }

    #[test]
    fn error_code_mapping() {
        assert_eq!(
            error_code(&GraphError::Timeout),
            "Neo.TransientError.Transaction.LockClientStopped"
        );
        assert_eq!(
            error_code(&GraphError::Validation("x".into())),
            "Neo.ClientError.Statement.SemanticError"
        );
        assert_eq!(
            error_code(&GraphError::PermissionDenied("x".into())),
            "Neo.ClientError.Security.Forbidden"
        );
        assert_eq!(
            error_code(&GraphError::Internal("x".into())),
            "Neo.DatabaseError.General.UnknownError"
        );
    }

    #[test]
    fn json_to_pack_value_primitives() {
        assert_eq!(
            json_to_pack_value(&serde_json::Value::Null),
            PackValue::Null
        );
        assert_eq!(
            json_to_pack_value(&serde_json::Value::Bool(true)),
            PackValue::Bool(true)
        );
        assert_eq!(
            json_to_pack_value(&serde_json::json!(42)),
            PackValue::Int(42)
        );
        assert_eq!(
            json_to_pack_value(&serde_json::json!(1.23)),
            PackValue::Float(1.23)
        );
        assert_eq!(
            json_to_pack_value(&serde_json::json!("hello")),
            PackValue::String("hello".into())
        );
    }

    #[test]
    fn json_to_pack_value_compound() {
        let arr = serde_json::json!([1, "two", null]);
        let packed = json_to_pack_value(&arr);
        match packed {
            PackValue::List(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], PackValue::Int(1));
                assert_eq!(items[1], PackValue::String("two".into()));
                assert_eq!(items[2], PackValue::Null);
            }
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn find_string_field_found() {
        let map = vec![
            ("name".into(), PackValue::String("Alice".into())),
            ("age".into(), PackValue::Int(30)),
        ];
        assert_eq!(find_string_field(&map, "name"), Some("Alice".into()));
        assert_eq!(find_string_field(&map, "age"), None); // Not a string
        assert_eq!(find_string_field(&map, "missing"), None);
    }
}
