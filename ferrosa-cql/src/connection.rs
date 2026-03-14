//! Per-connection CQL protocol handler.
//!
//! Implements the CQL v5 connection lifecycle:
//!
//! 1. **AwaitingStartup** — only STARTUP and OPTIONS are accepted.
//! 2. **Authenticating** — only AUTH_RESPONSE is accepted; max 3 attempts.
//! 3. **Ready** — QUERY, PREPARE, EXECUTE, BATCH, REGISTER accepted.
//!
//! Security mitigations:
//! - **M7**: State machine enforcement — wrong-phase opcodes return ERROR(Protocol).
//! - **M11**: Idle timeout of 300 seconds via `tokio::time::timeout()`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::StreamExt;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, warn};

use crate::auth::{
    encode_auth_success, encode_authenticate_response, parse_sasl_plain, MAX_AUTH_ATTEMPTS,
};
use crate::error::CqlError;
use crate::frame::{Compression, CqlCodec, CqlFrame, FrameHeader, Opcode, VERSION_RESPONSE};
use crate::parser;
use crate::prepared::{PreparedCache, PreparedPlan};
use crate::result;
use crate::router::{RequestContext, RouteResult, SharedState};
use crate::subscribe::SubscriptionState;
use crate::types::CqlType;
use crate::virtual_tables::connections::{ConnectionInfo, ConnectionTracker};

use ferrosa_schema::AuthContext;

use futures::SinkExt;

/// Idle timeout: drop connection if no complete frame arrives within this duration (M11).
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Connection phase state machine (M7).
#[derive(Debug)]
enum ConnectionPhase {
    /// Initial phase — only STARTUP and OPTIONS are allowed.
    AwaitingStartup,
    /// After STARTUP when auth is enabled — only AUTH_RESPONSE is allowed.
    Authenticating { attempts: u32 },
    /// After successful auth (or STARTUP with auth disabled) — queries allowed.
    Ready,
}

/// RAII guard that deregisters a connection from the tracker on drop.
///
/// This ensures the connection is always removed from the tracker, even if
/// the handler panics or returns early due to an error.
pub(crate) struct ConnectionGuard {
    tracker: Arc<ConnectionTracker>,
    peer: SocketAddr,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.tracker.deregister(&self.peer);
    }
}

/// Handle a single CQL connection.
///
/// This function owns the TCP connection and processes frames until the client
/// disconnects, an error occurs, or the idle timeout fires.
pub async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    max_frame_size: u32,
    auth_disabled: bool,
    state: Arc<SharedState>,
) {
    debug!("new connection from {peer}");

    // Register this connection with the tracker and create a drop guard.
    state.connection_tracker.register(
        peer,
        ConnectionInfo {
            peer_address: peer.ip().to_string(),
            peer_port: peer.port(),
            state: "startup".to_owned(),
            username: None,
            connected_at: Instant::now(),
            requests_served: 0,
            protocol_version: 5,
        },
    );
    let _guard = ConnectionGuard {
        tracker: state.connection_tracker.clone(),
        peer,
    };

    let codec = CqlCodec::new(max_frame_size);
    let mut framed = Framed::new(stream, codec);
    let mut phase = ConnectionPhase::AwaitingStartup;
    let mut auth_context: Option<AuthContext> = None;
    let mut current_keyspace: Option<String> = None;
    let mut subscription_state = SubscriptionState::new(8);
    let mut pending_compression: Option<Compression> = None;

    loop {
        // M11: idle timeout — drop connection if no frame arrives within IDLE_TIMEOUT.
        let maybe_frame = match timeout(IDLE_TIMEOUT, framed.next()).await {
            Ok(Some(Ok(frame))) => frame,
            Ok(Some(Err(e))) => {
                warn!("frame decode error from {peer}: {e}");
                break;
            }
            Ok(None) => {
                // Stream ended — client disconnected.
                debug!("connection from {peer} closed (EOF)");
                break;
            }
            Err(_) => {
                // Timeout — drop the connection.
                debug!("idle timeout for {peer}, closing connection");
                break;
            }
        };

        let stream_id = maybe_frame.header.stream_id;

        // Snapshot phase discriminant before handling, so we can detect transitions.
        let was_awaiting_startup = matches!(phase, ConnectionPhase::AwaitingStartup);
        let was_ready = matches!(phase, ConnectionPhase::Ready);

        match handle_frame(
            &mut phase,
            &mut auth_context,
            &mut current_keyspace,
            &state,
            auth_disabled,
            &maybe_frame,
            &mut pending_compression,
        )
        .await
        {
            HandleResult::Reply(opcode, body) => {
                // Track phase transitions and requests.
                if was_awaiting_startup && matches!(phase, ConnectionPhase::Authenticating { .. }) {
                    // STARTUP sent, auth required — entered Authenticating phase.
                    state
                        .connection_tracker
                        .update_state(&peer, "authenticating");
                } else if !was_ready && matches!(phase, ConnectionPhase::Ready) {
                    // Transitioned to Ready — update state and username if auth completed.
                    state.connection_tracker.update_state(&peer, "ready");
                    if let Some(ctx) = auth_context.as_ref() {
                        state.connection_tracker.update_username(&peer, &ctx.role);
                    }
                }
                if was_ready {
                    // Count every request handled in the Ready phase.
                    state.connection_tracker.increment_requests(&peer);
                }

                let body_bytes = body.freeze();
                let frame = CqlFrame {
                    header: FrameHeader {
                        version: VERSION_RESPONSE,
                        flags: 0,
                        stream_id,
                        opcode,
                        length: 0, // CqlCodec::encode will set this
                    },
                    body: body_bytes,
                };
                if framed.send(frame).await.is_err() {
                    break;
                }

                // After sending READY or AUTH_SUCCESS, enable compression if negotiated.
                // STARTUP/READY frames themselves are NOT compressed; only subsequent frames.
                if (opcode == Opcode::Ready || opcode == Opcode::AuthSuccess)
                    && pending_compression.is_some()
                {
                    let compression = pending_compression.take().unwrap();
                    debug!(
                        "enabling {} compression for {peer}",
                        compression.protocol_name()
                    );
                    framed.codec_mut().set_compression(compression);
                }
            }
            HandleResult::Close(opcode, body) => {
                let body_bytes = body.freeze();
                let frame = CqlFrame {
                    header: FrameHeader {
                        version: VERSION_RESPONSE,
                        flags: 0,
                        stream_id,
                        opcode,
                        length: 0,
                    },
                    body: body_bytes,
                };
                let _ = framed.send(frame).await;
                break;
            }
            HandleResult::CloseNow => {
                break;
            }
        }
    }

    // Cancel all active subscriptions on disconnect.
    subscription_state.cancel_all();

    debug!("connection handler for {peer} finished");
}

/// Outcome of processing a single frame.
enum HandleResult {
    /// Send a response and continue reading.
    Reply(Opcode, BytesMut),
    /// Send a response and then close the connection.
    Close(Opcode, BytesMut),
    /// Close immediately without sending anything (reserved for future use).
    #[allow(dead_code)]
    CloseNow,
}

/// Dispatch a single frame based on the current connection phase.
async fn handle_frame(
    phase: &mut ConnectionPhase,
    auth_context: &mut Option<AuthContext>,
    current_keyspace: &mut Option<String>,
    state: &SharedState,
    auth_disabled: bool,
    frame: &CqlFrame,
    pending_compression: &mut Option<Compression>,
) -> HandleResult {
    match phase {
        ConnectionPhase::AwaitingStartup => match frame.header.opcode {
            Opcode::Startup => {
                handle_startup(phase, auth_disabled, &frame.body, pending_compression)
            }
            Opcode::Options => handle_options(),
            _ => {
                let err = CqlError::Protocol(format!(
                    "unexpected opcode {:?} before STARTUP",
                    frame.header.opcode
                ));
                HandleResult::Reply(Opcode::Error, err.encode_body())
            }
        },
        ConnectionPhase::Authenticating { .. } => match frame.header.opcode {
            Opcode::AuthResponse => handle_auth_response(phase, auth_context, state, &frame.body),
            _ => {
                let err = CqlError::Protocol(format!(
                    "unexpected opcode {:?} during authentication",
                    frame.header.opcode
                ));
                HandleResult::Reply(Opcode::Error, err.encode_body())
            }
        },
        ConnectionPhase::Ready => match frame.header.opcode {
            Opcode::Query => handle_query(auth_context, current_keyspace, state, &frame.body).await,
            Opcode::Prepare => handle_prepare(auth_context, current_keyspace, state, &frame.body),
            Opcode::Execute => {
                handle_execute(auth_context, current_keyspace, state, &frame.body).await
            }
            Opcode::Batch => handle_batch(auth_context, current_keyspace, state, &frame.body).await,
            Opcode::Register => handle_register(),
            Opcode::Options => handle_options(),
            _ => {
                let err = CqlError::Protocol(format!(
                    "unexpected opcode {:?} in Ready phase",
                    frame.header.opcode
                ));
                HandleResult::Reply(Opcode::Error, err.encode_body())
            }
        },
    }
}

// ── STARTUP ─────────────────────────────────────────────────────────────

fn handle_startup(
    phase: &mut ConnectionPhase,
    auth_disabled: bool,
    body: &Bytes,
    pending_compression: &mut Option<Compression>,
) -> HandleResult {
    // Parse the string map from the STARTUP body.
    if body.len() < 2 {
        let err = CqlError::Protocol("STARTUP body too short".into());
        return HandleResult::Reply(Opcode::Error, err.encode_body());
    }

    let mut cursor = &body[..];
    let n_pairs = cursor.get_u16() as usize;

    let mut cql_version: Option<String> = None;
    let mut compression_name: Option<String> = None;
    for _ in 0..n_pairs {
        if cursor.remaining() < 2 {
            let err = CqlError::Protocol("STARTUP body truncated".into());
            return HandleResult::Reply(Opcode::Error, err.encode_body());
        }
        let key_len = cursor.get_u16() as usize;
        if cursor.remaining() < key_len {
            let err = CqlError::Protocol("STARTUP body truncated".into());
            return HandleResult::Reply(Opcode::Error, err.encode_body());
        }
        let key = std::str::from_utf8(&cursor[..key_len]).unwrap_or("");
        cursor.advance(key_len);

        if cursor.remaining() < 2 {
            let err = CqlError::Protocol("STARTUP body truncated".into());
            return HandleResult::Reply(Opcode::Error, err.encode_body());
        }
        let val_len = cursor.get_u16() as usize;
        if cursor.remaining() < val_len {
            let err = CqlError::Protocol("STARTUP body truncated".into());
            return HandleResult::Reply(Opcode::Error, err.encode_body());
        }
        let val = std::str::from_utf8(&cursor[..val_len]).unwrap_or("");
        cursor.advance(val_len);

        match key {
            "CQL_VERSION" => cql_version = Some(val.to_string()),
            "COMPRESSION" => compression_name = Some(val.to_string()),
            _ => {} // Ignore unknown keys per CQL spec.
        }
    }

    if cql_version.is_none() {
        let err = CqlError::Protocol("STARTUP missing CQL_VERSION".into());
        return HandleResult::Reply(Opcode::Error, err.encode_body());
    }

    // Validate and store the requested compression algorithm.
    if let Some(name) = compression_name {
        match Compression::from_protocol_name(&name) {
            Some(algo) => *pending_compression = Some(algo),
            None => {
                let err = CqlError::Protocol(format!("unsupported compression algorithm: {name}"));
                return HandleResult::Reply(Opcode::Error, err.encode_body());
            }
        }
    }

    if auth_disabled {
        *phase = ConnectionPhase::Ready;
        // Return READY (empty body).
        HandleResult::Reply(Opcode::Ready, BytesMut::new())
    } else {
        *phase = ConnectionPhase::Authenticating { attempts: 0 };
        // Return AUTHENTICATE with the authenticator class name.
        let body = BytesMut::from(&encode_authenticate_response()[..]);
        HandleResult::Reply(Opcode::Authenticate, body)
    }
}

// ── OPTIONS ──────────────────────────────────────────────────────────────

fn handle_options() -> HandleResult {
    // Encode a SUPPORTED string-multimap.
    // Format: [short n_keys]([short key_len][bytes key][short n_values]([short val_len][bytes val])*)*
    let mut body = BytesMut::new();
    body.put_u16(2); // 2 keys

    // CQL_VERSION
    let key = b"CQL_VERSION";
    body.put_u16(key.len() as u16);
    body.put_slice(key);
    body.put_u16(1); // 1 value
    let val = b"3.0.0";
    body.put_u16(val.len() as u16);
    body.put_slice(val);

    // COMPRESSION
    let key = b"COMPRESSION";
    body.put_u16(key.len() as u16);
    body.put_slice(key);
    body.put_u16(2); // 2 values
    let val1 = b"lz4";
    body.put_u16(val1.len() as u16);
    body.put_slice(val1);
    let val2 = b"snappy";
    body.put_u16(val2.len() as u16);
    body.put_slice(val2);

    HandleResult::Reply(Opcode::Supported, body)
}

// ── AUTH_RESPONSE ─────────────────────────────────────────────────────────

fn handle_auth_response(
    phase: &mut ConnectionPhase,
    auth_context: &mut Option<AuthContext>,
    state: &SharedState,
    body: &Bytes,
) -> HandleResult {
    // Parse the SASL payload: [int length][bytes payload]
    if body.len() < 4 {
        let err = CqlError::Protocol("AUTH_RESPONSE body too short".into());
        return HandleResult::Reply(Opcode::Error, err.encode_body());
    }

    let mut cursor = &body[..];
    let payload_len = cursor.get_i32();
    if payload_len < 0 || cursor.remaining() < payload_len as usize {
        let err = CqlError::BadCredentials;
        return increment_auth_attempts_and_reply(phase, err);
    }

    let payload = &cursor[..payload_len as usize];

    match parse_sasl_plain(payload) {
        Ok((username, password)) => match state.schema.authenticate(username, password) {
            Ok(ctx) => {
                *auth_context = Some(ctx);
                *phase = ConnectionPhase::Ready;
                let body = BytesMut::from(&encode_auth_success()[..]);
                HandleResult::Reply(Opcode::AuthSuccess, body)
            }
            Err(_) => {
                let err = CqlError::BadCredentials;
                increment_auth_attempts_and_reply(phase, err)
            }
        },
        Err(_) => {
            let err = CqlError::BadCredentials;
            increment_auth_attempts_and_reply(phase, err)
        }
    }
}

/// Increment auth attempts, and close connection if MAX_AUTH_ATTEMPTS reached.
fn increment_auth_attempts_and_reply(phase: &mut ConnectionPhase, err: CqlError) -> HandleResult {
    if let ConnectionPhase::Authenticating { attempts } = phase {
        *attempts += 1;
        if *attempts >= MAX_AUTH_ATTEMPTS {
            return HandleResult::Close(Opcode::Error, err.encode_body());
        }
    }
    HandleResult::Reply(Opcode::Error, err.encode_body())
}

// ── QUERY ────────────────────────────────────────────────────────────────

async fn handle_query(
    auth_context: &mut Option<AuthContext>,
    current_keyspace: &mut Option<String>,
    state: &SharedState,
    body: &Bytes,
) -> HandleResult {
    // Parse the query string: [int length][bytes query][short consistency][byte flags]...
    if body.len() < 4 {
        let err = CqlError::Protocol("QUERY body too short".into());
        return HandleResult::Reply(Opcode::Error, err.encode_body());
    }

    let mut cursor = &body[..];
    let query_len = cursor.get_i32();
    if query_len < 0 || cursor.remaining() < query_len as usize {
        let err = CqlError::Protocol("QUERY body truncated".into());
        return HandleResult::Reply(Opcode::Error, err.encode_body());
    }
    let query_bytes = &cursor[..query_len as usize];
    let query = match std::str::from_utf8(query_bytes) {
        Ok(q) => q,
        Err(e) => {
            let err = CqlError::Protocol(format!("QUERY: invalid UTF-8: {e}"));
            return HandleResult::Reply(Opcode::Error, err.encode_body());
        }
    };

    // Parse the CQL statement.
    let stmt = match parser::parse(query) {
        Ok(s) => s,
        Err(e) => {
            return HandleResult::Reply(Opcode::Error, e.encode_body());
        }
    };

    // Build an auth context for routing (use a default if auth was disabled).
    let ctx = build_request_context(auth_context, current_keyspace);

    match crate::router::route(state, &ctx, stmt).await {
        Ok(RouteResult::Result(body)) => HandleResult::Reply(Opcode::Result, body),
        Ok(RouteResult::SetKeyspace(ks, body)) => {
            *current_keyspace = Some(ks);
            HandleResult::Reply(Opcode::Result, body)
        }
        Err(e) => HandleResult::Reply(Opcode::Error, e.encode_body()),
    }
}

// ── PREPARE ──────────────────────────────────────────────────────────────

fn handle_prepare(
    _auth_context: &mut Option<AuthContext>,
    current_keyspace: &mut Option<String>,
    state: &SharedState,
    body: &Bytes,
) -> HandleResult {
    // Parse the query string: [int length][bytes query]
    if body.len() < 4 {
        let err = CqlError::Protocol("PREPARE body too short".into());
        return HandleResult::Reply(Opcode::Error, err.encode_body());
    }

    let mut cursor = &body[..];
    let query_len = cursor.get_i32();
    if query_len < 0 || cursor.remaining() < query_len as usize {
        let err = CqlError::Protocol("PREPARE body truncated".into());
        return HandleResult::Reply(Opcode::Error, err.encode_body());
    }
    let query_bytes = &cursor[..query_len as usize];
    let query = match std::str::from_utf8(query_bytes) {
        Ok(q) => q,
        Err(e) => {
            let err = CqlError::Protocol(format!("PREPARE: invalid UTF-8: {e}"));
            return HandleResult::Reply(Opcode::Error, err.encode_body());
        }
    };

    let stmt = match parser::parse(query) {
        Ok(s) => s,
        Err(e) => {
            return HandleResult::Reply(Opcode::Error, e.encode_body());
        }
    };

    let id = PreparedCache::compute_id(query);

    // Determine keyspace and table from the statement for the prepared metadata.
    let (table_ks, table_name) = extract_keyspace_table(&stmt, current_keyspace);

    // Build bound_columns from the statement (simplified — no full type inference).
    let bound_columns: Vec<(String, CqlType)> = Vec::new();

    // Build result_columns (simplified — empty for non-SELECT).
    let result_columns: Vec<(String, CqlType)> = Vec::new();

    let plan = PreparedPlan {
        id,
        query: query.to_string(),
        statement: stmt,
        keyspace: current_keyspace.clone(),
        result_columns: result_columns.clone(),
        bound_columns: bound_columns.clone(),
        table_keyspace: table_ks.clone(),
        table_name: table_name.clone(),
    };

    state.prepared_cache.insert(plan);

    let bound_names: Vec<String> = bound_columns.iter().map(|(n, _)| n.clone()).collect();
    let bound_types: Vec<CqlType> = bound_columns.iter().map(|(_, t)| t.clone()).collect();
    let result_names: Vec<String> = result_columns.iter().map(|(n, _)| n.clone()).collect();
    let result_types: Vec<CqlType> = result_columns.iter().map(|(_, t)| t.clone()).collect();

    let result_body = result::encode_prepared(
        &id,
        &bound_names,
        &bound_types,
        &result_names,
        &result_types,
        &table_ks,
        &table_name,
    );

    HandleResult::Reply(Opcode::Result, result_body)
}

// ── EXECUTE ──────────────────────────────────────────────────────────────

async fn handle_execute(
    auth_context: &mut Option<AuthContext>,
    current_keyspace: &mut Option<String>,
    state: &SharedState,
    body: &Bytes,
) -> HandleResult {
    // Parse the prepared ID: [short id_len][bytes id]
    if body.len() < 2 {
        let err = CqlError::Protocol("EXECUTE body too short".into());
        return HandleResult::Reply(Opcode::Error, err.encode_body());
    }

    let mut cursor = &body[..];
    let id_len = cursor.get_u16() as usize;
    if id_len != 16 || cursor.remaining() < 16 {
        let err = CqlError::Protocol("EXECUTE: invalid prepared ID length".into());
        return HandleResult::Reply(Opcode::Error, err.encode_body());
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&cursor[..16]);

    // Look up the prepared plan.
    let plan = match state.prepared_cache.get(&id) {
        Some(p) => p,
        None => {
            let err = CqlError::Unprepared(id);
            return HandleResult::Reply(Opcode::Error, err.encode_body());
        }
    };

    // Re-route the stored statement (simplified: no bound value substitution).
    let ctx = build_request_context(auth_context, current_keyspace);

    match crate::router::route(state, &ctx, plan.statement.clone()).await {
        Ok(RouteResult::Result(body)) => HandleResult::Reply(Opcode::Result, body),
        Ok(RouteResult::SetKeyspace(ks, body)) => {
            *current_keyspace = Some(ks);
            HandleResult::Reply(Opcode::Result, body)
        }
        Err(e) => HandleResult::Reply(Opcode::Error, e.encode_body()),
    }
}

// ── BATCH ────────────────────────────────────────────────────────────────

async fn handle_batch(
    auth_context: &mut Option<AuthContext>,
    current_keyspace: &mut Option<String>,
    state: &SharedState,
    body: &Bytes,
) -> HandleResult {
    // Parse batch: [byte batch_type][short n_statements]
    if body.len() < 3 {
        let err = CqlError::Protocol("BATCH body too short".into());
        return HandleResult::Reply(Opcode::Error, err.encode_body());
    }

    let mut cursor = &body[..];
    let _batch_type = cursor.get_u8();
    let n_statements = cursor.get_u16() as usize;

    if n_statements > 500 {
        let err = CqlError::Protocol("BATCH: too many statements (max 500)".into());
        return HandleResult::Reply(Opcode::Error, err.encode_body());
    }

    // Collect all statements first, then route them.
    let mut statements = Vec::with_capacity(n_statements);

    for _ in 0..n_statements {
        if cursor.remaining() < 1 {
            let err = CqlError::Protocol("BATCH body truncated".into());
            return HandleResult::Reply(Opcode::Error, err.encode_body());
        }
        let kind = cursor.get_u8();

        let stmt = if kind == 0 {
            // Inline query string: [int len][bytes query]
            if cursor.remaining() < 4 {
                let err = CqlError::Protocol("BATCH body truncated".into());
                return HandleResult::Reply(Opcode::Error, err.encode_body());
            }
            let query_len = cursor.get_i32();
            if query_len < 0 || cursor.remaining() < query_len as usize {
                let err = CqlError::Protocol("BATCH body truncated".into());
                return HandleResult::Reply(Opcode::Error, err.encode_body());
            }
            let query = match std::str::from_utf8(&cursor[..query_len as usize]) {
                Ok(q) => q.to_string(),
                Err(e) => {
                    let err = CqlError::Protocol(format!("BATCH: invalid UTF-8: {e}"));
                    return HandleResult::Reply(Opcode::Error, err.encode_body());
                }
            };
            cursor.advance(query_len as usize);

            match parser::parse(&query) {
                Ok(s) => s,
                Err(e) => {
                    return HandleResult::Reply(Opcode::Error, e.encode_body());
                }
            }
        } else if kind == 1 {
            // Prepared ID: [short id_len][bytes id]
            if cursor.remaining() < 2 {
                let err = CqlError::Protocol("BATCH body truncated".into());
                return HandleResult::Reply(Opcode::Error, err.encode_body());
            }
            let id_len = cursor.get_u16() as usize;
            if id_len != 16 || cursor.remaining() < 16 {
                let err = CqlError::Protocol("BATCH: invalid prepared ID".into());
                return HandleResult::Reply(Opcode::Error, err.encode_body());
            }
            let mut id = [0u8; 16];
            id.copy_from_slice(&cursor[..16]);
            cursor.advance(16);

            match state.prepared_cache.get(&id) {
                Some(p) => p.statement.clone(),
                None => {
                    let err = CqlError::Unprepared(id);
                    return HandleResult::Reply(Opcode::Error, err.encode_body());
                }
            }
        } else {
            let err = CqlError::Protocol(format!("BATCH: invalid statement kind {kind}"));
            return HandleResult::Reply(Opcode::Error, err.encode_body());
        };

        // Skip bound values: [short n_values]([int val_len][bytes val])*
        if cursor.remaining() >= 2 {
            let n_values = cursor.get_u16() as usize;
            for _ in 0..n_values {
                if cursor.remaining() < 4 {
                    break;
                }
                let val_len = cursor.get_i32();
                if val_len > 0 && cursor.remaining() >= val_len as usize {
                    cursor.advance(val_len as usize);
                }
            }
        }

        statements.push(stmt);
    }

    // Route each statement.
    for stmt in statements {
        let ctx = build_request_context(auth_context, current_keyspace);
        match crate::router::route(state, &ctx, stmt).await {
            Ok(RouteResult::SetKeyspace(ks, _)) => {
                *current_keyspace = Some(ks);
            }
            Ok(_) => {}
            Err(e) => {
                return HandleResult::Reply(Opcode::Error, e.encode_body());
            }
        }
    }

    // BATCH returns a void result.
    HandleResult::Reply(Opcode::Result, result::encode_void())
}

// ── REGISTER ─────────────────────────────────────────────────────────────

fn handle_register() -> HandleResult {
    // Accept registration and return READY. Event push is deferred.
    HandleResult::Reply(Opcode::Ready, BytesMut::new())
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Build a `RequestContext` from the current auth context and keyspace.
fn build_request_context<'a>(
    auth_context: &'a mut Option<AuthContext>,
    current_keyspace: &'a Option<String>,
) -> RequestContext<'a> {
    // If auth was disabled, we need a default auth context.
    if auth_context.is_none() {
        *auth_context = Some(AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        });
    }
    RequestContext {
        auth: auth_context.as_ref().unwrap(),
        current_keyspace,
    }
}

/// Extract keyspace and table name from a statement for prepared metadata.
fn extract_keyspace_table(
    stmt: &crate::ast::Statement,
    current_keyspace: &Option<String>,
) -> (String, String) {
    use crate::ast::Statement;
    let default_ks = current_keyspace.clone().unwrap_or_default();

    match stmt {
        Statement::Select(s) => (
            s.keyspace.clone().unwrap_or_else(|| default_ks.clone()),
            s.table.clone(),
        ),
        Statement::Insert(i) => (
            i.keyspace.clone().unwrap_or_else(|| default_ks.clone()),
            i.table.clone(),
        ),
        Statement::Update(u) => (
            u.keyspace.clone().unwrap_or_else(|| default_ks.clone()),
            u.table.clone(),
        ),
        Statement::Delete(d) => (
            d.keyspace.clone().unwrap_or_else(|| default_ks.clone()),
            d.table.clone(),
        ),
        _ => (default_ks, String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_guard_deregisters_on_drop() {
        let tracker = Arc::new(ConnectionTracker::new());
        let addr: SocketAddr = "127.0.0.1:9042".parse().unwrap();
        tracker.register(
            addr,
            ConnectionInfo {
                peer_address: "127.0.0.1".to_owned(),
                peer_port: 9042,
                state: "startup".to_owned(),
                username: None,
                connected_at: Instant::now(),
                requests_served: 0,
                protocol_version: 5,
            },
        );
        assert_eq!(tracker.active_count(), 1);
        {
            let _guard = ConnectionGuard {
                tracker: tracker.clone(),
                peer: addr,
            };
        }
        assert_eq!(tracker.active_count(), 0);
    }

    #[test]
    fn connection_guard_deregisters_on_panic_unwind() {
        let tracker = Arc::new(ConnectionTracker::new());
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        tracker.register(
            addr,
            ConnectionInfo {
                peer_address: "127.0.0.1".to_owned(),
                peer_port: 12345,
                state: "startup".to_owned(),
                username: None,
                connected_at: Instant::now(),
                requests_served: 0,
                protocol_version: 5,
            },
        );
        assert_eq!(tracker.active_count(), 1);

        let tracker_clone = tracker.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = ConnectionGuard {
                tracker: tracker_clone,
                peer: addr,
            };
            panic!("simulated panic");
        }));
        assert!(result.is_err());
        assert_eq!(tracker.active_count(), 0);
    }

    #[test]
    fn handle_options_advertises_compression() {
        let result = handle_options();
        match result {
            HandleResult::Reply(opcode, body) => {
                assert_eq!(opcode, Opcode::Supported);
                // Parse the string-multimap to verify COMPRESSION key.
                let mut cursor = &body[..];
                let n_keys = cursor.get_u16();
                assert_eq!(n_keys, 2);

                // First key: CQL_VERSION
                let key_len = cursor.get_u16() as usize;
                let key = std::str::from_utf8(&cursor[..key_len]).unwrap();
                cursor.advance(key_len);
                assert_eq!(key, "CQL_VERSION");
                let n_vals = cursor.get_u16();
                assert_eq!(n_vals, 1);
                let val_len = cursor.get_u16() as usize;
                cursor.advance(val_len); // skip value

                // Second key: COMPRESSION
                let key_len = cursor.get_u16() as usize;
                let key = std::str::from_utf8(&cursor[..key_len]).unwrap();
                cursor.advance(key_len);
                assert_eq!(key, "COMPRESSION");
                let n_vals = cursor.get_u16();
                assert_eq!(n_vals, 2);
                let val_len = cursor.get_u16() as usize;
                let val1 = std::str::from_utf8(&cursor[..val_len]).unwrap();
                cursor.advance(val_len);
                let val_len = cursor.get_u16() as usize;
                let val2 = std::str::from_utf8(&cursor[..val_len]).unwrap();
                assert_eq!(val1, "lz4");
                assert_eq!(val2, "snappy");
            }
            _ => panic!("expected Reply"),
        }
    }

    #[test]
    fn handle_startup_with_compression() {
        // Build a STARTUP body with CQL_VERSION and COMPRESSION keys.
        let mut body = BytesMut::new();
        body.put_u16(2); // 2 key-value pairs

        let key = b"CQL_VERSION";
        body.put_u16(key.len() as u16);
        body.put_slice(key);
        let val = b"3.0.0";
        body.put_u16(val.len() as u16);
        body.put_slice(val);

        let key = b"COMPRESSION";
        body.put_u16(key.len() as u16);
        body.put_slice(key);
        let val = b"lz4";
        body.put_u16(val.len() as u16);
        body.put_slice(val);

        let mut phase = ConnectionPhase::AwaitingStartup;
        let mut pending = None;
        let result = handle_startup(&mut phase, true, &body.freeze(), &mut pending);
        assert!(matches!(result, HandleResult::Reply(Opcode::Ready, _)));
        assert_eq!(pending, Some(Compression::Lz4));
    }

    #[test]
    fn handle_startup_with_invalid_compression() {
        let mut body = BytesMut::new();
        body.put_u16(2);

        let key = b"CQL_VERSION";
        body.put_u16(key.len() as u16);
        body.put_slice(key);
        let val = b"3.0.0";
        body.put_u16(val.len() as u16);
        body.put_slice(val);

        let key = b"COMPRESSION";
        body.put_u16(key.len() as u16);
        body.put_slice(key);
        let val = b"zstd";
        body.put_u16(val.len() as u16);
        body.put_slice(val);

        let mut phase = ConnectionPhase::AwaitingStartup;
        let mut pending = None;
        let result = handle_startup(&mut phase, true, &body.freeze(), &mut pending);
        assert!(matches!(result, HandleResult::Reply(Opcode::Error, _)));
        assert!(pending.is_none());
    }
}
