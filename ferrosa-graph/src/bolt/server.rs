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

/// Configuration for the Bolt server.
#[derive(Debug, Clone)]
pub struct BoltConfig {
    /// Address to bind to (default: 127.0.0.1:7687).
    pub bind_addr: SocketAddr,
    /// Maximum message size in bytes.
    pub max_message_size: usize,
    /// Maximum concurrent connections.
    pub max_connections: usize,
    /// When true, skip credential validation and authenticate as a superuser.
    pub auth_disabled: bool,
}

impl Default for BoltConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:7687".parse().unwrap(),
            max_message_size: 16 * 1024 * 1024, // 16MB
            max_connections: 256,
            auth_disabled: false,
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
    /// The unconsumed remainder of the running query, as a stream.
    ///
    /// Held as a stream rather than a `GraphResult` so the server does not keep
    /// the whole result in memory between PULLs. The client already paged
    /// correctly; the server did not.
    pending_result: Option<PendingRows>,
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
            // Extract optional keyspace / database selection. Bolt 5 official
            // drivers send authentication in a subsequent LOGON message, not
            // necessarily in HELLO.
            if let Some(db) = find_string_field(&extra, "db") {
                state.keyspace = db;
            }

            // Backward-compatible path for older clients that still include
            // credentials in HELLO.
            if find_string_field(&extra, "principal").is_some()
                || find_string_field(&extra, "credentials").is_some()
            {
                if let Err(e) =
                    authenticate_bolt_fields(&extra, &schema, config.auth_disabled, &mut state)
                        .await
                {
                    tracing::debug!(%e, "Bolt HELLO authentication failed");
                    send_message(&mut stream, &auth_failure_message()).await?;
                    return Ok(());
                }
            }

            let success = BoltMessage::Success {
                metadata: vec![
                    ("server".into(), PackValue::String("Ferrosa/0.1.0".into())),
                    ("connection_id".into(), PackValue::String("bolt-1".into())),
                ],
            };
            send_message(&mut stream, &success).await?;
        }
        _ => {
            return Err(GraphError::Internal(
                "expected HELLO as first message".into(),
            ));
        }
    }

    // Handle optional LOGON message (Bolt 5+). If the next message is not
    // LOGON, process it as a regular message; clients that authenticated via
    // HELLO can start RUN/PULL immediately.
    let logon_data = read_message(&mut stream, &mut decoder, config.max_message_size).await?;
    let logon_msg = BoltMessage::decode(&logon_data)
        .map_err(|e| GraphError::Internal(format!("LOGON decode: {e}")))?;

    match logon_msg {
        BoltMessage::Logon { auth } => {
            if let Err(e) =
                authenticate_bolt_fields(&auth, &schema, config.auth_disabled, &mut state).await
            {
                tracing::debug!(%e, "Bolt LOGON authentication failed");
                send_message(&mut stream, &auth_failure_message()).await?;
                return Ok(());
            }
            let success = BoltMessage::Success { metadata: vec![] };
            send_message(&mut stream, &success).await?;
        }
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
                            "neo4j_code".into(),
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

/// Handle a Bolt explicit-transaction control message (`BEGIN` / `COMMIT` /
/// `ROLLBACK`).
///
/// URS-QEC-B01 wires these message types into the dispatch path. The connection
/// transaction state machine (URS-QEC-B02) — queueing statements, deferring
/// execution to `COMMIT`, and backing it with the `StorageEngine` batch
/// primitive — lands in the next increment. Until then these messages must
/// **fail loud** (URS-QEC-X01): an explicit transaction we cannot actually open
/// or durably commit must return a Bolt `FAILURE`, never a silent `SUCCESS` that
/// would ack a transaction we never persisted.
fn handle_tx_message(msg: &BoltMessage, _state: &mut ConnectionState) -> Vec<BoltMessage> {
    let (code, message) = match msg {
        BoltMessage::Begin { .. } => (
            "Neo.ClientError.Transaction.TransactionStartFailed",
            "explicit transactions (BEGIN) are not yet supported on this server",
        ),
        BoltMessage::Commit => (
            "Neo.ClientError.Transaction.TransactionNotFound",
            "COMMIT received with no open explicit transaction \
             (explicit transactions are not yet supported)",
        ),
        BoltMessage::Rollback => (
            "Neo.ClientError.Transaction.TransactionNotFound",
            "ROLLBACK received with no open explicit transaction \
             (explicit transactions are not yet supported)",
        ),
        // Not a transaction control message — caller guarantees this is unreachable.
        _ => (
            "Neo.DatabaseError.General.UnknownError",
            "handle_tx_message called with a non-transaction message",
        ),
    };
    vec![BoltMessage::Failure {
        metadata: vec![
            ("code".into(), PackValue::String(code.into())),
            ("neo4j_code".into(), PackValue::String(code.into())),
            ("message".into(), PackValue::String(message.into())),
        ],
    }]
}

/// Process a single message and produce response messages.
async fn process_message(
    msg: BoltMessage,
    engine: &Arc<GraphEngine>,
    _schema: &Arc<Schema>,
    state: &mut ConnectionState,
) -> Result<Vec<BoltMessage>, GraphError> {
    match msg {
        BoltMessage::Begin { .. } | BoltMessage::Commit | BoltMessage::Rollback => {
            Ok(handle_tx_message(&msg, state))
        }

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

            let params = pack_params_to_json(params);

            match engine
                .execute_stream_with_params(&query, &keyspace, &auth, &params)
                .await
            {
                Ok((columns, rows, stats)) => {
                    let fields: Vec<PackValue> = columns
                        .iter()
                        .map(|c| PackValue::String(c.clone()))
                        .collect();
                    let success = BoltMessage::Success {
                        metadata: vec![
                            ("fields".into(), PackValue::List(fields)),
                            ("t_first".into(), PackValue::Int(stats.execution_ms as i64)),
                            ("qid".into(), PackValue::Int(0)),
                        ],
                    };
                    state.pending_result = Some(PendingRows {
                        stats,
                        rows,
                        peeked: None,
                    });
                    Ok(vec![success])
                }
                Err(e) => {
                    let code = error_code(&e);
                    let failure = BoltMessage::Failure {
                        metadata: vec![
                            ("code".into(), PackValue::String(code.into())),
                            ("neo4j_code".into(), PackValue::String(code.into())),
                            ("message".into(), PackValue::String(e.to_string())),
                        ],
                    };
                    Ok(vec![failure])
                }
            }
        }

        BoltMessage::Pull { extra } => {
            if let Some(mut result) = state.pending_result.take() {
                let mut replies = Vec::new();

                // Honor the protocol's `n`: PULL {n} must deliver AT MOST n
                // records and signal `has_more` when the result is not
                // exhausted, so a client can page a large result instead of
                // receiving all of it in one batch. `n` absent or negative
                // (canonically -1) means "fetch all" (t_4ce82a3e).
                //
                // Previously `extra` was discarded and every row was sent with
                // has_more hardcoded false, so a client asking for 10 rows got
                // the entire result — a protocol violation, and unbounded
                // client-side memory for a large query.
                //
                // The remainder is now a stream, so the SERVER no longer holds
                // every undelivered row either: this pulls `n` from the query
                // and looks one past them to answer `has_more`.
                let (batch, has_more) = result.take_batch(requested_batch_size(&extra)).await?;

                // Send a RECORD for each row in this batch.
                for row in &batch {
                    let values: Vec<PackValue> = row.iter().map(json_to_pack_value).collect();
                    replies.push(BoltMessage::Record { values });
                }

                // Keep the rest for the next PULL. Stats travel with the FINAL
                // batch, matching Bolt's summary semantics.
                if has_more {
                    // The stream is positioned at the next undelivered row, so
                    // this is a move of a cursor, not of the remaining rows.
                    state.pending_result = Some(result);
                    replies.push(BoltMessage::Success {
                        metadata: vec![("has_more".into(), PackValue::Bool(true))],
                    });
                    return Ok(replies);
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

        BoltMessage::Discard { extra } => {
            // DISCARD {n} discards AT MOST n records, exactly mirroring PULL {n}
            // — it is not "throw the whole result away". Previously `extra` was
            // discarded and the entire pending result was dropped, so a client
            // discarding one record lost the rest with no way to reach it.
            // `n` absent or negative means discard everything (t_4ce82a3e).
            match state.pending_result.take() {
                Some(mut result) => {
                    // DISCARD {n} drops n records and keeps the rest; it is
                    // not "throw the whole result away". Pulling them from the
                    // stream discards without materialising them.
                    let (_dropped, has_more) =
                        result.take_batch(requested_batch_size(&extra)).await?;
                    if has_more {
                        state.pending_result = Some(result);
                        return Ok(vec![BoltMessage::Success {
                            metadata: vec![("has_more".into(), PackValue::Bool(true))],
                        }]);
                    }
                    Ok(vec![BoltMessage::Success { metadata: vec![] }])
                }
                None => Ok(vec![BoltMessage::Success { metadata: vec![] }]),
            }
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

/// A Bolt `FAILURE` for an authentication error. Sending this (rather than just
/// dropping the connection) is what makes a Bolt driver raise an auth error
/// instead of a generic `SessionExpired` / `ServiceUnavailable`.
fn auth_failure_message() -> BoltMessage {
    BoltMessage::Failure {
        metadata: vec![
            (
                "code".into(),
                PackValue::String("Neo.ClientError.Security.Unauthorized".into()),
            ),
            (
                "neo4j_code".into(),
                PackValue::String("Neo.ClientError.Security.Unauthorized".into()),
            ),
            (
                "message".into(),
                PackValue::String("authentication failed".into()),
            ),
        ],
    }
}

async fn authenticate_bolt_fields(
    fields: &[(String, PackValue)],
    schema: &Arc<Schema>,
    auth_disabled: bool,
    state: &mut ConnectionState,
) -> Result<(), GraphError> {
    let username =
        find_string_field(fields, "principal").unwrap_or_else(|| "anonymous".to_string());
    let password = find_string_field(fields, "credentials").unwrap_or_default();

    let auth_ctx = if auth_disabled {
        AuthContext {
            role: username,
            is_superuser: true,
            must_change_password: false,
        }
    } else {
        match schema.authenticate(&username, &password) {
            Ok(auth_ctx) => auth_ctx,
            Err(_) => {
                return Err(GraphError::PermissionDenied(
                    "authentication failed".to_string(),
                ));
            }
        }
    };

    state.authenticated = true;
    state.auth_context = Some(auth_ctx);
    Ok(())
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

/// Convert Bolt RUN parameter values to JSON values consumed by the Cypher engine.
fn pack_params_to_json(
    params: Vec<(String, PackValue)>,
) -> std::collections::HashMap<String, serde_json::Value> {
    params
        .into_iter()
        .filter(|(name, _)| name != "db")
        .map(|(name, value)| (name, pack_to_json_value(&value)))
        .collect()
}

fn pack_to_json_value(v: &PackValue) -> serde_json::Value {
    match v {
        PackValue::Null => serde_json::Value::Null,
        PackValue::Bool(b) => serde_json::Value::Bool(*b),
        PackValue::Int(i) => serde_json::Value::Number((*i).into()),
        PackValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        PackValue::Bytes(bytes) => serde_json::Value::Array(
            bytes
                .iter()
                .map(|b| serde_json::Value::Number((*b as u64).into()))
                .collect(),
        ),
        PackValue::String(s) => serde_json::Value::String(s.clone()),
        PackValue::List(values) => {
            serde_json::Value::Array(values.iter().map(pack_to_json_value).collect())
        }
        PackValue::Map(entries) => serde_json::Value::Object(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), pack_to_json_value(v)))
                .collect(),
        ),
        PackValue::Structure { .. } => serde_json::Value::Null,
    }
}

/// Convert a `serde_json::Value` to a `PackValue`.
/// How many records this `PULL` may deliver: `Some(n)` for a bounded batch,
/// `None` for "fetch all".
///
/// Pure (takes the decoded `extra` map, does no I/O) so the protocol rule is
/// unit-testable without a socket or a session — the risky part of PULL paging
/// is this parse, not the row copying.
///
/// Bolt semantics: `n` absent, negative (canonically `-1`), or non-integer means
/// fetch everything. `n >= 0` bounds the batch; `n == 0` legitimately yields an
/// empty batch with `has_more` still set.
fn requested_batch_size(extra: &[(String, PackValue)]) -> Option<usize> {
    match extra.iter().find(|(k, _)| k == "n").map(|(_, v)| v) {
        Some(PackValue::Int(n)) if *n >= 0 => Some(*n as usize),
        _ => None,
    }
}

/// Decide one PULL batch: `(rows_to_send, has_more)` for a `requested` batch
/// size (`None` = fetch all) against `total` buffered rows.
///
/// Pure so the paging arithmetic — the part with the off-by-one and
/// saturation edges — is unit-testable without a GraphEngine or a socket.
/// Consume at most `requested` records from `rows`, returning the consumed
/// batch and whether any records remain.
///
/// PULL and DISCARD share this contract exactly and differ only in what they do
/// with the batch — PULL serializes it into RECORDs, DISCARD drops it — so the
/// consumption lives here rather than being written twice and drifting.
/// A query whose rows have not all been delivered yet.
///
/// Bolt's PULL is inherently incremental — the client asks for `n` records at a
/// time — so the natural shape for the remainder is a stream, not a `Vec`. It
/// used to be a `GraphResult`: the client paged, and the server held every row
/// of every open result until the last PULL.
///
/// Holding the stream also keeps the ORDER BY spill alive for exactly as long
/// as it can still be pulled from. `SpillSortSink::finish_stream` moves the
/// temp-dir reservation into the stream, so the spilled runs are removed when
/// this is dropped — a client that disconnects mid-result cleans up with it.
struct PendingRows {
    stats: crate::executor::result::QueryStats,
    rows: crate::executor::RowStream<'static>,
    /// One row pulled ahead to answer `has_more` without consuming it.
    ///
    /// A stream cannot say whether it is empty without being polled, and Bolt
    /// must report `has_more` in the same SUCCESS as the batch. Pulling one
    /// extra and holding it costs a single row.
    peeked: Option<Vec<serde_json::Value>>,
}

impl PendingRows {
    /// Take up to `requested` rows, reporting whether any remain.
    ///
    /// `None` means "fetch all" (Bolt's canonical `n = -1`).
    async fn take_batch(
        &mut self,
        requested: Option<usize>,
    ) -> Result<(Vec<Vec<serde_json::Value>>, bool), GraphError> {
        use futures::StreamExt as _;

        let mut batch = Vec::new();
        let wanted = requested.unwrap_or(usize::MAX);

        while batch.len() < wanted {
            let next = match self.peeked.take() {
                Some(row) => Some(row),
                None => match self.rows.next().await {
                    Some(row) => Some(row?),
                    None => None,
                },
            };
            match next {
                Some(row) => batch.push(row),
                None => return Ok((batch, false)),
            }
        }

        // Look one past the batch so `has_more` is accurate without delivering
        // a row the client did not ask for.
        let has_more = match self.rows.next().await {
            Some(row) => {
                self.peeked = Some(row?);
                true
            }
            None => false,
        };
        Ok((batch, has_more))
    }
}

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
        GraphError::ConstraintViolation(_) => "Neo.ClientError.Schema.ConstraintValidationFailed",
        GraphError::Timeout => "Neo.TransientError.Transaction.LockClientStopped",
        GraphError::Storage(_) | GraphError::Schema(_) | GraphError::Internal(_) => {
            "Neo.DatabaseError.General.UnknownError"
        }
    }
}

#[cfg(test)]
mod tests {

    /// PULL and DISCARD share one contract: consume at most `n` records from
    /// the pending result and report whether any remain. They differ only in
    /// what they do with the consumed batch (serialize it vs drop it), so the
    /// consumption itself lives in one tested function — a second copy in the
    /// DISCARD arm is how the two drift apart.
    #[tokio::test]
    async fn take_batch_consumes_at_most_n_and_leaves_the_remainder() {
        fn pending(n: i64) -> PendingRows {
            PendingRows {
                stats: Default::default(),
                rows: crate::executor::stream::stream_from_rows(
                    (1..=n).map(|i| vec![serde_json::json!(i)]).collect(),
                ),
                peeked: None,
            }
        }

        // Partial: batch is the first n, the rest stay reachable.
        let mut r = pending(5);
        let (batch, has_more) = r.take_batch(Some(2)).await.unwrap();
        assert_eq!(
            batch,
            vec![vec![serde_json::json!(1)], vec![serde_json::json!(2)]]
        );
        assert!(has_more, "3 rows remain");
        let (rest, has_more) = r.take_batch(None).await.unwrap();
        assert_eq!(
            rest,
            vec![
                vec![serde_json::json!(3)],
                vec![serde_json::json!(4)],
                vec![serde_json::json!(5)],
            ],
            "the peeked row must be delivered, not dropped"
        );
        assert!(!has_more);

        // n == len: an exact drain must NOT report more, or the client loops
        // forever asking for rows that do not exist.
        let mut r = pending(5);
        let (batch, has_more) = r.take_batch(Some(5)).await.unwrap();
        assert_eq!(batch.len(), 5);
        assert!(!has_more);

        // Fetch-all (n absent / negative) drains everything.
        let mut r = pending(5);
        let (batch, has_more) = r.take_batch(None).await.unwrap();
        assert_eq!(batch.len(), 5);
        assert!(!has_more);

        // n == 0 consumes nothing but must still report more, else the
        // remaining rows become unreachable.
        let mut r = pending(5);
        let (batch, has_more) = r.take_batch(Some(0)).await.unwrap();
        assert!(batch.is_empty());
        assert!(has_more);
        let (rest, _) = r.take_batch(None).await.unwrap();
        assert_eq!(rest.len(), 5, "nothing was consumed by the zero-batch");

        // Empty result: no panic, nothing more.
        let mut r = pending(0);
        let (batch, has_more) = r.take_batch(Some(3)).await.unwrap();
        assert!(batch.is_empty());
        assert!(!has_more);
    }

    /// Bolt PULL {n} must deliver AT MOST n records. `n` absent/negative means
    /// "fetch all". Before this parse existed the whole `extra` map was
    /// discarded, so EVERY row was sent regardless of `n` — a protocol
    /// violation and unbounded client-side memory (t_4ce82a3e inc 7). These
    /// cases are exactly what the old `Pull { .. }` could not express.
    #[test]
    fn requested_batch_size_honors_the_protocol_n() {
        use crate::bolt::codec::PackValue;

        // Bounded batch.
        assert_eq!(
            requested_batch_size(&[("n".to_string(), PackValue::Int(10))]),
            Some(10)
        );
        // n = 0 is legal: an empty batch, has_more still decides continuation.
        assert_eq!(
            requested_batch_size(&[("n".to_string(), PackValue::Int(0))]),
            Some(0)
        );
        // -1 is the canonical "fetch all".
        assert_eq!(
            requested_batch_size(&[("n".to_string(), PackValue::Int(-1))]),
            None
        );
        // Any other negative also means fetch-all rather than panicking on the
        // `as usize` cast.
        assert_eq!(
            requested_batch_size(&[("n".to_string(), PackValue::Int(-99))]),
            None
        );
        // Absent -> fetch all (what every pre-existing client sends).
        assert_eq!(requested_batch_size(&[]), None);
        // Wrong type -> fetch all rather than failing the pull.
        assert_eq!(
            requested_batch_size(&[("n".to_string(), PackValue::String("10".into()))]),
            None
        );
        // Unrelated keys (qid etc.) are ignored.
        assert_eq!(
            requested_batch_size(&[
                ("qid".to_string(), PackValue::Int(1)),
                ("n".to_string(), PackValue::Int(3)),
            ]),
            Some(3)
        );
    }
    use super::*;

    #[test]
    fn bolt_config_default() {
        let config = BoltConfig::default();
        assert_eq!(
            config.bind_addr,
            "127.0.0.1:7687".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(config.max_message_size, 16 * 1024 * 1024);
        assert_eq!(config.max_connections, 256);
        assert!(!config.auth_disabled);
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

    /// Extract the `code` field from a FAILURE message, if present.
    fn failure_code(msg: &BoltMessage) -> Option<String> {
        match msg {
            BoltMessage::Failure { metadata } => find_string_field(metadata, "code"),
            _ => None,
        }
    }

    /// BEGIN must NOT silently SUCCEED while the transaction state machine is
    /// unimplemented — it must fail loud (URS-QEC-X01).
    #[test]
    fn begin_fails_loud_not_silent_success() {
        let mut state = ConnectionState::new();
        let replies = handle_tx_message(&BoltMessage::Begin { extra: vec![] }, &mut state);
        assert_eq!(replies.len(), 1, "expected exactly one reply");
        assert!(
            matches!(replies[0], BoltMessage::Failure { .. }),
            "BEGIN must return FAILURE, never SUCCESS, until the tx state machine exists; got {:?}",
            replies[0]
        );
        assert_eq!(
            failure_code(&replies[0]).as_deref(),
            Some("Neo.ClientError.Transaction.TransactionStartFailed"),
        );
    }

    /// COMMIT with no open transaction must fail loud — never a fake SUCCESS that
    /// acks a transaction it never persisted.
    #[test]
    fn commit_without_tx_fails_loud() {
        let mut state = ConnectionState::new();
        let replies = handle_tx_message(&BoltMessage::Commit, &mut state);
        assert_eq!(replies.len(), 1);
        assert!(
            matches!(replies[0], BoltMessage::Failure { .. }),
            "COMMIT must return FAILURE, got {:?}",
            replies[0]
        );
    }

    /// ROLLBACK with no open transaction must fail loud, not silently succeed.
    #[test]
    fn rollback_without_tx_fails_loud() {
        let mut state = ConnectionState::new();
        let replies = handle_tx_message(&BoltMessage::Rollback, &mut state);
        assert_eq!(replies.len(), 1);
        assert!(
            matches!(replies[0], BoltMessage::Failure { .. }),
            "ROLLBACK must return FAILURE, got {:?}",
            replies[0]
        );
    }

    /// Every FAILURE we emit for tx messages carries both `code` and `message`
    /// fields so drivers surface a real error rather than hanging.
    #[test]
    fn tx_failures_carry_code_and_message() {
        for msg in [
            BoltMessage::Begin { extra: vec![] },
            BoltMessage::Commit,
            BoltMessage::Rollback,
        ] {
            let mut state = ConnectionState::new();
            let replies = handle_tx_message(&msg, &mut state);
            match &replies[0] {
                BoltMessage::Failure { metadata } => {
                    assert!(
                        find_string_field(metadata, "code").is_some(),
                        "missing code for {msg:?}"
                    );
                    assert!(
                        find_string_field(metadata, "message").is_some(),
                        "missing message for {msg:?}"
                    );
                }
                other => panic!("expected FAILURE for {msg:?}, got {other:?}"),
            }
        }
    }
}
