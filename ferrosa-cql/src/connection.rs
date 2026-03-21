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
///
/// In-flight request limiting: a semaphore bounds the number of concurrent
/// requests being processed. When the limit is reached, new requests receive
/// ERROR(Overloaded) without consuming a permit.
pub async fn handle_connection<S>(
    stream: S,
    peer: SocketAddr,
    max_frame_size: u32,
    max_in_flight: usize,
    auth_disabled: bool,
    state: Arc<SharedState>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
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

    // Channel for subscription tasks to push streaming frames.
    let (sub_tx, mut sub_rx) = tokio::sync::mpsc::channel::<crate::subscribe::SubscriptionPush>(64);

    // In-flight request limiter: bounds concurrent requests on this connection.
    let in_flight = tokio::sync::Semaphore::new(max_in_flight);

    loop {
        // Use select to handle both client frames and subscription pushes.
        let frame_or_push = tokio::select! {
            // M11: idle timeout — drop connection if no frame arrives within IDLE_TIMEOUT.
            result = timeout(IDLE_TIMEOUT, framed.next()) => {
                match result {
                    Ok(Some(Ok(frame))) => FrameOrPush::ClientFrame(frame),
                    Ok(Some(Err(e))) => {
                        warn!("frame decode error from {peer}: {e}");
                        break;
                    }
                    Ok(None) => {
                        debug!("connection from {peer} closed (EOF)");
                        break;
                    }
                    Err(_) => {
                        debug!("idle timeout for {peer}, closing connection");
                        break;
                    }
                }
            }
            Some(push) = sub_rx.recv() => {
                FrameOrPush::SubscriptionPush(push)
            }
        };

        match frame_or_push {
            FrameOrPush::SubscriptionPush(push) => {
                // Send streaming result frame from a subscription task.
                let frame = CqlFrame {
                    header: FrameHeader::streaming_response(push.stream_id, Opcode::Result),
                    body: push.body,
                };
                if framed.send(frame).await.is_err() {
                    break;
                }
            }
            FrameOrPush::ClientFrame(maybe_frame) => {
                let stream_id = maybe_frame.header.stream_id;

                // Check in-flight limit for request opcodes (QUERY, EXECUTE, BATCH).
                let is_request = matches!(
                    maybe_frame.header.opcode,
                    Opcode::Query | Opcode::Execute | Opcode::Batch
                );
                let _permit = if is_request {
                    match in_flight.try_acquire() {
                        Ok(permit) => Some(permit),
                        Err(_) => {
                            debug!("in-flight limit reached for {peer}, rejecting request");
                            let err = CqlError::Overloaded("request backpressure".into());
                            let body = err.encode_body().freeze();
                            let frame = CqlFrame {
                                header: FrameHeader {
                                    version: VERSION_RESPONSE,
                                    flags: 0,
                                    stream_id,
                                    opcode: Opcode::Error,
                                    length: 0,
                                },
                                body,
                            };
                            if framed.send(frame).await.is_err() {
                                break;
                            }
                            continue;
                        }
                    }
                } else {
                    None
                };

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
                        if was_awaiting_startup
                            && matches!(phase, ConnectionPhase::Authenticating { .. })
                        {
                            state
                                .connection_tracker
                                .update_state(&peer, "authenticating");
                        } else if !was_ready && matches!(phase, ConnectionPhase::Ready) {
                            state.connection_tracker.update_state(&peer, "ready");
                            if let Some(ctx) = auth_context.as_ref() {
                                state.connection_tracker.update_username(&peer, &ctx.role);
                            }
                        }
                        if was_ready {
                            state.connection_tracker.increment_requests(&peer);
                        }

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
                        if framed.send(frame).await.is_err() {
                            break;
                        }

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
                    HandleResult::StartSubscription {
                        inner,
                        interval,
                        delta: _,
                    } => {
                        let interval = match interval {
                            Some(d) => d,
                            None => {
                                // Change-driven mode not yet implemented
                                let err = CqlError::Invalid(
                                    "SUBSCRIBE without EVERY not yet supported; \
                                     use SUBSCRIBE ... EVERY <interval>"
                                        .into(),
                                );
                                let body = err.encode_body().freeze();
                                let frame = CqlFrame {
                                    header: FrameHeader {
                                        version: VERSION_RESPONSE,
                                        flags: 0,
                                        stream_id,
                                        opcode: Opcode::Error,
                                        length: 0,
                                    },
                                    body,
                                };
                                if framed.send(frame).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                        };

                        // Create a cancellation token and register the subscription.
                        let cancel = tokio_util::sync::CancellationToken::new();
                        let handle = crate::subscribe::SubscriptionHandle {
                            stream_id: stream_id as u16,
                            cancel: cancel.clone(),
                        };
                        if let Err(msg) = subscription_state.add(handle) {
                            let err = CqlError::Invalid(msg.to_string());
                            let body = err.encode_body().freeze();
                            let frame = CqlFrame {
                                header: FrameHeader {
                                    version: VERSION_RESPONSE,
                                    flags: 0,
                                    stream_id,
                                    opcode: Opcode::Error,
                                    length: 0,
                                },
                                body,
                            };
                            if framed.send(frame).await.is_err() {
                                break;
                            }
                            continue;
                        }

                        // Send void ACK to confirm subscription.
                        let ack_body = crate::result::encode_void().freeze();
                        let frame = CqlFrame {
                            header: FrameHeader {
                                version: VERSION_RESPONSE,
                                flags: 0,
                                stream_id,
                                opcode: Opcode::Result,
                                length: 0,
                            },
                            body: ack_body,
                        };
                        if framed.send(frame).await.is_err() {
                            break;
                        }

                        // Build owned auth context for the subscription task.
                        let auth = auth_context.clone().unwrap_or(AuthContext {
                            role: "cassandra".to_string(),
                            is_superuser: true,
                            must_change_password: false,
                        });

                        crate::subscribe::spawn_subscription_poll(
                            stream_id,
                            interval,
                            state.clone(),
                            auth,
                            current_keyspace.clone(),
                            *inner,
                            sub_tx.clone(),
                            cancel,
                        );

                        debug!(
                            "subscription started for {peer} stream={stream_id} interval={:?}",
                            interval
                        );
                    }
                    HandleResult::CancelSubscription { stream_id: sub_id } => {
                        subscription_state.cancel(sub_id);
                        let body = crate::result::encode_void().freeze();
                        let frame = CqlFrame {
                            header: FrameHeader {
                                version: VERSION_RESPONSE,
                                flags: 0,
                                stream_id,
                                opcode: Opcode::Result,
                                length: 0,
                            },
                            body,
                        };
                        if framed.send(frame).await.is_err() {
                            break;
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
        }
    }

    // Cancel all active subscriptions on disconnect.
    subscription_state.cancel_all();

    debug!("connection handler for {peer} finished");
}

/// Internal enum for the select! loop — either a client frame or a subscription push.
enum FrameOrPush {
    ClientFrame(CqlFrame),
    SubscriptionPush(crate::subscribe::SubscriptionPush),
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
    /// Subscription accepted — send void ACK, then spawn polling task.
    StartSubscription {
        inner: Box<crate::ast::Statement>,
        interval: Option<Duration>,
        #[allow(dead_code)] // delta mode deferred
        delta: bool,
    },
    /// Unsubscribe — cancel one or all subscriptions.
    CancelSubscription { stream_id: Option<u16> },
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
    cursor.advance(query_len as usize);

    // Parse consistency level from protocol frame: [short consistency]
    let cl = if cursor.remaining() >= 2 {
        let wire_cl = cursor.get_u16();
        ferrosa_cluster::consistency::ConsistencyLevel::from_wire(wire_cl)
            .unwrap_or(ferrosa_cluster::consistency::ConsistencyLevel::One)
    } else {
        ferrosa_cluster::consistency::ConsistencyLevel::One
    };

    // Parse the CQL statement.
    let stmt = match parser::parse(query) {
        Ok(s) => s,
        Err(e) => {
            return HandleResult::Reply(Opcode::Error, e.encode_body());
        }
    };

    // Build an auth context for routing (use a default if auth was disabled).
    let ctx = build_request_context(auth_context, current_keyspace, cl);

    match crate::router::route(state, &ctx, stmt).await {
        Ok(RouteResult::Result(body)) => HandleResult::Reply(Opcode::Result, body),
        Ok(RouteResult::SetKeyspace(ks, body)) => {
            *current_keyspace = Some(ks);
            HandleResult::Reply(Opcode::Result, body)
        }
        Ok(RouteResult::Subscribe {
            inner,
            interval,
            delta,
        }) => HandleResult::StartSubscription {
            inner,
            interval,
            delta,
        },
        Ok(RouteResult::Unsubscribe { stream_id }) => {
            HandleResult::CancelSubscription { stream_id }
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

    // Build bound_columns: resolve bind marker column names to their CQL types
    // from the table schema.
    let bind_col_names = extract_bind_column_names(&stmt);
    let bound_columns: Vec<(String, CqlType)> = {
        let snapshot = state.schema.snapshot();
        let key = (table_ks.clone(), table_name.clone());
        if let Some(table_meta) = snapshot.tables.get(&key) {
            bind_col_names
                .iter()
                .map(|col_name| {
                    let cql_type = table_meta
                        .columns
                        .get(col_name)
                        .map(|cm| col_type_str_to_cql_type(&cm.column_type))
                        .unwrap_or(CqlType::Blob);
                    (col_name.clone(), cql_type)
                })
                .collect()
        } else {
            Vec::new()
        }
    };

    // Build result_columns for SELECT statements.
    let result_columns: Vec<(String, CqlType)> = Vec::new();

    // Compute pk_indexes: map each partition key column to its position in the
    // bind variable list. If any PK column is not bound, pass empty (pk_count=0).
    let pk_indexes = compute_pk_indexes(&stmt, state, &table_ks, &table_name);

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
        &pk_indexes,
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
    cursor.advance(16);

    // Parse consistency level from EXECUTE frame: after the prepared ID
    // comes [short consistency][byte flags][...values...].
    let cl = if cursor.remaining() >= 2 {
        let wire_cl = cursor.get_u16();
        ferrosa_cluster::consistency::ConsistencyLevel::from_wire(wire_cl)
            .unwrap_or(ferrosa_cluster::consistency::ConsistencyLevel::One)
    } else {
        ferrosa_cluster::consistency::ConsistencyLevel::One
    };

    // Look up the prepared plan.
    let plan = match state.prepared_cache.get(&id) {
        Some(p) => p,
        None => {
            let err = CqlError::Unprepared(id);
            return HandleResult::Reply(Opcode::Error, err.encode_body());
        }
    };

    // Re-route the stored statement (simplified: no bound value substitution).
    let ctx = build_request_context(auth_context, current_keyspace, cl);

    match crate::router::route(state, &ctx, plan.statement.clone()).await {
        Ok(RouteResult::Result(body)) => HandleResult::Reply(Opcode::Result, body),
        Ok(RouteResult::SetKeyspace(ks, body)) => {
            *current_keyspace = Some(ks);
            HandleResult::Reply(Opcode::Result, body)
        }
        Ok(RouteResult::Subscribe { .. } | RouteResult::Unsubscribe { .. }) => {
            // SUBSCRIBE/UNSUBSCRIBE via EXECUTE is not supported — use QUERY.
            let err = CqlError::Invalid("SUBSCRIBE via EXECUTE not supported".into());
            HandleResult::Reply(Opcode::Error, err.encode_body())
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

    // Parse consistency level: [short consistency] after all statements.
    let cl = if cursor.remaining() >= 2 {
        let wire_cl = cursor.get_u16();
        ferrosa_cluster::consistency::ConsistencyLevel::from_wire(wire_cl)
            .unwrap_or(ferrosa_cluster::consistency::ConsistencyLevel::One)
    } else {
        ferrosa_cluster::consistency::ConsistencyLevel::One
    };

    // Route each statement.
    for stmt in statements {
        let ctx = build_request_context(auth_context, current_keyspace, cl);
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
    consistency: ferrosa_cluster::consistency::ConsistencyLevel,
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
        consistency,
    }
}

/// Compute partition-key bind-variable indexes for a prepared statement.
///
/// Looks up the table's partition key columns in the schema, then finds each
/// PK column's position in the statement's bind variable list. Returns the
/// indexes in partition key order. If the table is not found in the schema or
/// any PK column does not appear as a bind variable, returns an empty vec
/// (which writes `pk_count=0`, disabling token-aware routing in drivers).
fn compute_pk_indexes(
    stmt: &crate::ast::Statement,
    state: &SharedState,
    table_ks: &str,
    table_name: &str,
) -> Vec<u16> {
    if table_ks.is_empty() || table_name.is_empty() {
        return Vec::new();
    }

    // Look up the table in the schema to get partition key column names.
    let snapshot = state.schema.snapshot();
    let key = (table_ks.to_string(), table_name.to_string());
    let table_meta = match snapshot.tables.get(&key) {
        Some(t) => t,
        None => return Vec::new(),
    };

    if table_meta.partition_key.is_empty() {
        return Vec::new();
    }

    // Extract the ordered list of column names that have bind markers.
    let bind_columns = extract_bind_column_names(stmt);

    // For each PK column (in partition key order), find its index in bind_columns.
    let mut pk_indexes = Vec::with_capacity(table_meta.partition_key.len());
    for pk_col in &table_meta.partition_key {
        match bind_columns.iter().position(|name| name == pk_col) {
            Some(idx) => pk_indexes.push(idx as u16),
            None => return Vec::new(), // PK column not bound — disable routing
        }
    }

    pk_indexes
}

/// Extract the ordered list of column names that have bind markers in a statement.
///
/// The returned order matches the bind variable order in the CQL protocol:
/// - INSERT: columns whose corresponding value is a `BindMarker`, in column order
/// - SELECT/UPDATE/DELETE: `WHERE` clause columns with bind markers, in clause order
/// - UPDATE: assignments with bind markers come before WHERE bind markers
fn extract_bind_column_names(stmt: &crate::ast::Statement) -> Vec<String> {
    use crate::ast::{Assignment, Statement, Term};

    match stmt {
        Statement::Insert(ins) => {
            let mut names = Vec::new();
            for (col, val) in ins.columns.iter().zip(ins.values.iter()) {
                if matches!(val, Term::BindMarker(_)) {
                    names.push(col.clone());
                }
            }
            names
        }
        Statement::Select(sel) => {
            let mut names = Vec::new();
            for wc in &sel.where_clauses {
                if matches!(&wc.value, Term::BindMarker(_)) {
                    names.push(wc.column.clone());
                }
            }
            names
        }
        Statement::Update(upd) => {
            let mut names = Vec::new();
            // SET assignments first (bind variables in assignment order)
            for assign in &upd.assignments {
                match assign {
                    Assignment::Simple {
                        column,
                        value: Term::BindMarker(_),
                    }
                    | Assignment::Add {
                        column,
                        value: Term::BindMarker(_),
                    }
                    | Assignment::Sub {
                        column,
                        value: Term::BindMarker(_),
                    } => {
                        names.push(column.clone());
                    }
                    Assignment::Element { column, key, value } => {
                        if matches!(key, Term::BindMarker(_)) {
                            names.push(column.clone());
                        }
                        if matches!(value, Term::BindMarker(_)) {
                            names.push(column.clone());
                        }
                    }
                    _ => {}
                }
            }
            // WHERE clauses after assignments
            for wc in &upd.where_clauses {
                if matches!(&wc.value, Term::BindMarker(_)) {
                    names.push(wc.column.clone());
                }
            }
            names
        }
        Statement::Delete(del) => {
            let mut names = Vec::new();
            for wc in &del.where_clauses {
                if matches!(&wc.value, Term::BindMarker(_)) {
                    names.push(wc.column.clone());
                }
            }
            names
        }
        _ => Vec::new(),
    }
}

/// Map a column type string (from schema metadata) to the CQL wire type.
fn col_type_str_to_cql_type(type_str: &str) -> CqlType {
    match type_str.to_lowercase().as_str() {
        "ascii" => CqlType::Ascii,
        "bigint" => CqlType::Bigint,
        "blob" => CqlType::Blob,
        "boolean" => CqlType::Boolean,
        "counter" => CqlType::Counter,
        "decimal" => CqlType::Decimal,
        "double" => CqlType::Double,
        "float" => CqlType::Float,
        "int" => CqlType::Int,
        "timestamp" => CqlType::Timestamp,
        "uuid" => CqlType::Uuid,
        "varchar" | "text" => CqlType::Varchar,
        "varint" => CqlType::Varint,
        "timeuuid" => CqlType::Timeuuid,
        "inet" => CqlType::Inet,
        "date" => CqlType::Date,
        "time" => CqlType::Time,
        "smallint" => CqlType::Smallint,
        "tinyint" => CqlType::Tinyint,
        "duration" => CqlType::Duration,
        _ => CqlType::Blob, // fallback for complex/unknown types
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
