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
use tracing::{debug, warn, Instrument};

use crate::ast::{Assignment, SelectColumn, Statement, Term};
use crate::auth::{
    encode_auth_success, encode_authenticate_response, parse_sasl_plain, MAX_AUTH_ATTEMPTS,
};
use crate::bridge;
use crate::error::CqlError;
use crate::frame::{Compression, CqlCodec, CqlFrame, FrameHeader, Opcode, VERSION_RESPONSE};
use crate::parser;
use crate::prepared::{PreparedCache, PreparedPlan};
use crate::result;
use crate::router::{RequestContext, RouteResult, SharedState};
use crate::subscribe::SubscriptionState;
use crate::types::{decode_value, CqlType, CqlValue};
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
    let mut client_protocol_version: u8 = 4; // default; updated from STARTUP frame

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
                    Ok(Some(Err(CqlError::ProtocolVersionMismatch { requested, supported }))) => {
                        warn!(
                            "protocol version mismatch from {peer}: \
                             requested v{requested}, max supported v{supported}"
                        );
                        // Send an ERROR response using v4 framing so the
                        // driver knows to fall back to a lower version.
                        let err = CqlError::ProtocolVersionMismatch { requested, supported };
                        let body = err.encode_body();
                        let resp = CqlFrame {
                            header: FrameHeader {
                                version: VERSION_RESPONSE,
                                flags: 0,
                                stream_id: 0,
                                opcode: Opcode::Error,
                                length: body.len() as u32,
                            },
                            body: body.freeze(),
                        };
                        let _ = framed.send(resp).await;
                        break;
                    }
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

                // Track the client's protocol version from the first frame.
                if matches!(phase, ConnectionPhase::AwaitingStartup) {
                    client_protocol_version = maybe_frame.header.version;
                }
                let was_awaiting_startup = matches!(phase, ConnectionPhase::AwaitingStartup);
                let was_ready = matches!(phase, ConnectionPhase::Ready);

                let request_span = tracing::info_span!(
                    "cql.request",
                    cql.opcode = ?maybe_frame.header.opcode,
                    client.address = %peer,
                );

                match async {
                    handle_frame(
                        &mut phase,
                        &mut auth_context,
                        &mut current_keyspace,
                        &state,
                        auth_disabled,
                        &maybe_frame,
                        &mut pending_compression,
                        peer,
                    )
                    .await
                }
                .instrument(request_span)
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
                        // Use the correct response version: 0x84 for v4, 0x85 for v5.
                        let response_version = if client_protocol_version >= 0x05 {
                            0x85
                        } else {
                            VERSION_RESPONSE // 0x84
                        };
                        let frame = CqlFrame {
                            header: FrameHeader {
                                version: response_version,
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

                        // After READY or AUTH_SUCCESS, enable post-handshake features.
                        if opcode == Opcode::Ready || opcode == Opcode::AuthSuccess {
                            if let Some(compression) = pending_compression.take() {
                                debug!(
                                    "enabling {} compression for {peer}",
                                    compression.protocol_name()
                                );
                                framed.codec_mut().set_compression(compression);
                            }
                            // CQL v5 switches to framed mode (CRC24/CRC32) after READY.
                            if client_protocol_version >= 0x05 {
                                debug!("enabling v5 framing for {peer}");
                                framed.codec_mut().enable_v5_framing();
                            }
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
pub(crate) enum HandleResult {
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
#[allow(clippy::too_many_arguments)]
async fn handle_frame(
    phase: &mut ConnectionPhase,
    auth_context: &mut Option<AuthContext>,
    current_keyspace: &mut Option<String>,
    state: &SharedState,
    auth_disabled: bool,
    frame: &CqlFrame,
    pending_compression: &mut Option<Compression>,
    peer: SocketAddr,
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
            Opcode::Query => {
                handle_query(auth_context, current_keyspace, state, &frame.body, peer).await
            }
            Opcode::Prepare => handle_prepare(auth_context, current_keyspace, state, &frame.body),
            Opcode::Execute => {
                handle_execute(auth_context, current_keyspace, state, &frame.body, peer).await
            }
            Opcode::Batch => {
                handle_batch(auth_context, current_keyspace, state, &frame.body, peer).await
            }
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
    peer: SocketAddr,
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
    let mut stmt = match parser::parse(query) {
        Ok(s) => s,
        Err(e) => {
            return HandleResult::Reply(Opcode::Error, e.encode_body());
        }
    };

    // Parse bound values from the QUERY frame (if present) and substitute
    // into the statement. cdrs-tokio sends query_with_values which includes
    // values in the QUERY frame alongside bind markers in the query text.
    if cursor.remaining() > 0 {
        let (table_ks, table_name) = extract_keyspace_table(&stmt, current_keyspace);
        let (bound_columns, _result_columns) =
            analyze_prepared_columns(&stmt, &table_ks, &table_name, state);
        // Build a temporary plan for substitution.
        let temp_plan = PreparedPlan {
            id: [0u8; 16],
            query: query.to_string(),
            statement: stmt,
            keyspace: current_keyspace.clone(),
            result_columns: Vec::new(),
            bound_columns,
            table_keyspace: table_ks,
            table_name,
        };
        stmt = match substitute_bound_values(&temp_plan, cursor) {
            Ok(s) => s,
            Err(e) => {
                return HandleResult::Reply(Opcode::Error, e.encode_body());
            }
        };
    }

    // Build an auth context for routing (use a default if auth was disabled).
    let ctx = build_request_context(auth_context, current_keyspace, cl, peer);

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

pub(crate) fn handle_prepare(
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

    // Build bound_columns and result_columns from the statement + schema metadata.
    let (bound_columns, result_columns) =
        analyze_prepared_columns(&stmt, &table_ks, &table_name, state);

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
        &[],
    );

    HandleResult::Reply(Opcode::Result, result_body)
}

// ── EXECUTE ──────────────────────────────────────────────────────────────

async fn handle_execute(
    auth_context: &mut Option<AuthContext>,
    current_keyspace: &mut Option<String>,
    state: &SharedState,
    body: &Bytes,
    peer: SocketAddr,
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

    // Parse consistency level: [short consistency]
    // Note: CQL v4 EXECUTE has no result_metadata_id field.
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

    // Parse bound values from the EXECUTE frame and substitute into the AST.
    let stmt = match substitute_bound_values(&plan, cursor) {
        Ok(s) => s,
        Err(e) => {
            return HandleResult::Reply(Opcode::Error, e.encode_body());
        }
    };

    let ctx = build_request_context(auth_context, current_keyspace, cl, peer);

    match crate::router::route(state, &ctx, stmt).await {
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
    peer: SocketAddr,
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
        let ctx = build_request_context(auth_context, current_keyspace, cl, peer);
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
    peer: std::net::SocketAddr,
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
        serial_consistency: None,
        paging: crate::paging::PagingParams::default(),
        client_address: peer.to_string(),
    }
}

// ── Bound value analysis and substitution ────────────────────────────────

/// Column spec: ordered list of (column_name, CqlType).
type ColumnSpec = Vec<(String, CqlType)>;

/// Analyze a prepared statement AST to find columns that have `BindMarker`
/// values, and resolve their types from the schema. Also builds result
/// columns for SELECT statements.
///
/// Returns `(bound_columns, result_columns)` as ordered lists of
/// `(column_name, CqlType)`. Falls back to empty vectors if schema
/// lookup fails (the statement might target a table that doesn't exist yet).
fn analyze_prepared_columns(
    stmt: &Statement,
    table_ks: &str,
    table_name: &str,
    state: &SharedState,
) -> (ColumnSpec, ColumnSpec) {
    // If keyspace or table is empty, we can't look up metadata.
    if table_ks.is_empty() || table_name.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let snap = state.schema.snapshot();
    let table_meta = match snap
        .tables
        .get(&(table_ks.to_string(), table_name.to_string()))
    {
        Some(tm) => tm,
        None => return (Vec::new(), Vec::new()),
    };

    let resolve = |col_name: &str| -> Option<CqlType> {
        let col = table_meta.columns.get(col_name)?;
        bridge::parse_cql_type_in_keyspace(&col.column_type, table_ks, &state.schema).ok()
    };

    let mut bound_columns = Vec::new();

    match stmt {
        Statement::Select(s) => {
            // Bind markers in WHERE clauses
            for wc in &s.where_clauses {
                if matches!(wc.value, Term::BindMarker(_)) {
                    if let Some(cql_type) = resolve(&wc.column) {
                        bound_columns.push((wc.column.clone(), cql_type));
                    }
                }
            }

            // Build result columns from the SELECT column list
            let result_columns = build_result_columns(&s.columns, table_meta, table_ks, state);
            return (bound_columns, result_columns);
        }
        Statement::Insert(i) => {
            // Bind markers in VALUES, paired with column names
            for (idx, val) in i.values.iter().enumerate() {
                if matches!(val, Term::BindMarker(_)) {
                    if let Some(col_name) = i.columns.get(idx) {
                        if let Some(cql_type) = resolve(col_name) {
                            bound_columns.push((col_name.clone(), cql_type));
                        }
                    }
                }
            }
        }
        Statement::Update(u) => {
            // Bind markers in SET assignments
            for assignment in &u.assignments {
                match assignment {
                    Assignment::Simple {
                        column,
                        value: Term::BindMarker(_),
                    } => {
                        if let Some(cql_type) = resolve(column) {
                            bound_columns.push((column.clone(), cql_type));
                        }
                    }
                    Assignment::Add {
                        column,
                        value: Term::BindMarker(_),
                    } => {
                        if let Some(cql_type) = resolve(column) {
                            bound_columns.push((column.clone(), cql_type));
                        }
                    }
                    Assignment::Sub {
                        column,
                        value: Term::BindMarker(_),
                    } => {
                        if let Some(cql_type) = resolve(column) {
                            bound_columns.push((column.clone(), cql_type));
                        }
                    }
                    _ => {}
                }
            }
            // Bind markers in WHERE clauses
            for wc in &u.where_clauses {
                if matches!(wc.value, Term::BindMarker(_)) {
                    if let Some(cql_type) = resolve(&wc.column) {
                        bound_columns.push((wc.column.clone(), cql_type));
                    }
                }
            }
            // Bind markers in IF conditions (LWT)
            for cond in &u.if_conditions {
                if matches!(cond.value, Term::BindMarker(_)) {
                    if let Some(cql_type) = resolve(&cond.column) {
                        bound_columns.push((cond.column.clone(), cql_type));
                    }
                }
            }
        }
        Statement::Delete(d) => {
            // Bind markers in WHERE clauses
            for wc in &d.where_clauses {
                if matches!(wc.value, Term::BindMarker(_)) {
                    if let Some(cql_type) = resolve(&wc.column) {
                        bound_columns.push((wc.column.clone(), cql_type));
                    }
                }
            }
        }
        _ => {}
    }

    (bound_columns, Vec::new())
}

/// Build result column metadata for SELECT statements.
fn build_result_columns(
    select_columns: &[SelectColumn],
    table_meta: &ferrosa_schema::TableMetadata,
    table_ks: &str,
    state: &SharedState,
) -> Vec<(String, CqlType)> {
    let resolve = |col_name: &str| -> Option<CqlType> {
        let col = table_meta.columns.get(col_name)?;
        bridge::parse_cql_type_in_keyspace(&col.column_type, table_ks, &state.schema).ok()
    };

    let has_star = select_columns
        .iter()
        .any(|c| matches!(c, SelectColumn::Star));

    if has_star {
        return table_meta
            .columns
            .iter()
            .filter_map(|(name, col)| {
                bridge::parse_cql_type_in_keyspace(&col.column_type, table_ks, &state.schema)
                    .ok()
                    .map(|t| (name.clone(), t))
            })
            .collect();
    }

    let mut result = Vec::new();
    for sc in select_columns {
        match sc {
            SelectColumn::Star => unreachable!(),
            SelectColumn::Column(name) => {
                if let Some(cql_type) = resolve(name) {
                    result.push((name.clone(), cql_type));
                }
            }
            SelectColumn::FunctionCall { alias, name, .. } => {
                // For function calls, use the alias or function name;
                // type is unknown without deeper analysis, default to Varchar.
                let col_name = alias.as_ref().unwrap_or(name).clone();
                result.push((col_name, CqlType::Varchar));
            }
        }
    }
    result
}

/// Convert a decoded `CqlValue` back into a parser-level `Term` for AST substitution.
///
/// This is the reverse of `bridge::term_to_cql_value`. We only need to cover the
/// scalar types that appear as bind-variable values; collections and UDTs are
/// passed through as blob literals (the router will re-decode them).
fn cql_value_to_term(v: &CqlValue) -> Term {
    match v {
        CqlValue::Null => Term::Null,
        CqlValue::Int(n) => Term::IntegerLiteral(*n as i64),
        CqlValue::Bigint(n) | CqlValue::Counter(n) | CqlValue::Timestamp(n) => {
            Term::IntegerLiteral(*n)
        }
        CqlValue::Smallint(n) => Term::IntegerLiteral(*n as i64),
        CqlValue::Tinyint(n) => Term::IntegerLiteral(*n as i64),
        CqlValue::Float(bits) => Term::FloatLiteral(f32::from_bits(*bits) as f64),
        CqlValue::Double(bits) => Term::FloatLiteral(f64::from_bits(*bits)),
        CqlValue::Text(s) | CqlValue::Ascii(s) => Term::StringLiteral(s.clone()),
        CqlValue::Boolean(b) => Term::BoolLiteral(*b),
        CqlValue::Uuid(u) | CqlValue::Timeuuid(u) => Term::UuidLiteral(*u),
        CqlValue::Blob(b) => Term::BlobLiteral(b.clone()),
        CqlValue::Inet(addr) => Term::StringLiteral(addr.to_string()),
        CqlValue::Date(d) => Term::IntegerLiteral(*d as i64),
        CqlValue::Time(t) => Term::IntegerLiteral(*t),
        CqlValue::Varint(big) => {
            // Best-effort: try to fit into i64, otherwise render as string.
            match i64::try_from(big) {
                Ok(n) => Term::IntegerLiteral(n),
                Err(_) => Term::StringLiteral(big.to_string()),
            }
        }
        CqlValue::Decimal { .. } => {
            // Decimals don't have a direct Term representation; use string.
            Term::StringLiteral(format!("{v:?}"))
        }
        CqlValue::Duration { .. } => Term::StringLiteral(format!("{v:?}")),
        // Collections: encode back to blob bytes so the router re-decodes them.
        CqlValue::List(items) => Term::ListLiteral(items.iter().map(cql_value_to_term).collect()),
        CqlValue::Set(items) => Term::SetLiteral(items.iter().map(cql_value_to_term).collect()),
        CqlValue::Map(pairs) => Term::MapLiteral(
            pairs
                .iter()
                .map(|(k, v)| (cql_value_to_term(k), cql_value_to_term(v)))
                .collect(),
        ),
        CqlValue::Tuple(elems) => Term::TupleLiteral(
            elems
                .iter()
                .map(|e| match e {
                    Some(v) => cql_value_to_term(v),
                    None => Term::Null,
                })
                .collect(),
        ),
        CqlValue::Vector(_) | CqlValue::Udt(_) => {
            // Opaque types: pass as debug string. The router handles these
            // through the typed path, not AST substitution.
            Term::StringLiteral(format!("{v:?}"))
        }
    }
}

/// Convert raw CQL wire-format bytes for a bind value into a parser-level [`Term`].
///
/// For scalar types, parses the bytes to the appropriate typed `Term`.
/// For collection types (`Map`, `List`, `Set`), the bytes are passed through
/// as a [`Term::BlobLiteral`] so that they are stored verbatim in the SSTable
/// cell. The storage layer and read path both use the CQL wire format for
/// collections, so parsing them into structured Terms and re-serializing would
/// risk format drift (BUG-025 / BUG-026).
fn raw_bytes_to_term(cql_type: &CqlType, bytes: &[u8]) -> Term {
    match cql_type {
        // Collection types: pass bytes through unchanged for storage fidelity.
        CqlType::Map(_, _) | CqlType::List(_) | CqlType::Set(_) => {
            Term::BlobLiteral(bytes.to_vec())
        }
        // All other types: decode to a typed CqlValue, then convert to Term.
        _ => match decode_value(cql_type, bytes) {
            Ok(cql_value) => cql_value_to_term(&cql_value),
            Err(_) => Term::BlobLiteral(bytes.to_vec()),
        },
    }
}

/// Parse bound values from the EXECUTE frame cursor and substitute them
/// into the prepared statement AST, replacing `BindMarker` nodes with
/// literal `Term` values.
///
/// The cursor should be positioned after the consistency level bytes.
/// CQL v5 EXECUTE frame layout after consistency:
///   `[int flags][short n_values]([int len][bytes value])*`
fn substitute_bound_values(plan: &PreparedPlan, mut cursor: &[u8]) -> Result<Statement, CqlError> {
    // Parse flags byte (CQL v4: 1 byte). Bit 0x01 = Values present.
    if cursor.remaining() < 1 {
        return Ok(plan.statement.clone());
    }
    let flags = cursor.get_u8();
    let has_values = (flags & 0x01) != 0;

    if !has_values {
        return Ok(plan.statement.clone());
    }

    // Parse [short n_values]
    // Per CQL v4 spec 4.1.4, values come first after flags, before
    // optional fields (page_size, paging_state, etc.).
    if cursor.remaining() < 2 {
        return Err(CqlError::Protocol("EXECUTE: truncated values count".into()));
    }
    let n_values = cursor.get_u16() as usize;

    // Decode each bound value using the type from bound_columns.
    let mut bound_terms: Vec<Term> = Vec::with_capacity(n_values);
    for i in 0..n_values {
        if cursor.remaining() < 4 {
            return Err(CqlError::Protocol("EXECUTE: truncated value length".into()));
        }
        let val_len = cursor.get_i32();
        if val_len < 0 {
            // Null value
            bound_terms.push(Term::Null);
        } else {
            let val_len = val_len as usize;
            if cursor.remaining() < val_len {
                return Err(CqlError::Protocol("EXECUTE: truncated value bytes".into()));
            }
            let val_bytes = &cursor[..val_len];
            cursor.advance(val_len);

            // Look up the type for this positional value.
            let cql_type = if i < plan.bound_columns.len() {
                &plan.bound_columns[i].1
            } else {
                // More values than bound columns — default to blob.
                &CqlType::Blob
            };

            bound_terms.push(raw_bytes_to_term(cql_type, val_bytes));
        }
    }

    // Walk the statement AST and replace BindMarker nodes in order.
    let mut substitution_idx = 0usize;
    Ok(substitute_in_statement(
        &plan.statement,
        &bound_terms,
        &mut substitution_idx,
    ))
}

/// Recursively walk a statement and replace `Term::BindMarker` with bound terms.
fn substitute_in_statement(stmt: &Statement, terms: &[Term], idx: &mut usize) -> Statement {
    match stmt {
        Statement::Select(s) => {
            let mut s = s.clone();
            for wc in &mut s.where_clauses {
                substitute_in_term(&mut wc.value, terms, idx);
            }
            Statement::Select(s)
        }
        Statement::Insert(i) => {
            let mut i = i.clone();
            for val in &mut i.values {
                substitute_in_term(val, terms, idx);
            }
            Statement::Insert(i)
        }
        Statement::Update(u) => {
            let mut u = u.clone();
            for assignment in &mut u.assignments {
                match assignment {
                    Assignment::Simple { value, .. } => substitute_in_term(value, terms, idx),
                    Assignment::Add { value, .. } => substitute_in_term(value, terms, idx),
                    Assignment::Sub { value, .. } => substitute_in_term(value, terms, idx),
                    Assignment::Element { key, value, .. } => {
                        substitute_in_term(key, terms, idx);
                        substitute_in_term(value, terms, idx);
                    }
                }
            }
            for wc in &mut u.where_clauses {
                substitute_in_term(&mut wc.value, terms, idx);
            }
            Statement::Update(u)
        }
        Statement::Delete(d) => {
            let mut d = d.clone();
            for wc in &mut d.where_clauses {
                substitute_in_term(&mut wc.value, terms, idx);
            }
            Statement::Delete(d)
        }
        // For other statement types, return as-is.
        other => other.clone(),
    }
}

/// Replace a `Term::BindMarker` with the next bound term from the list.
/// Recurses into nested terms (lists, maps, sets, tuples).
fn substitute_in_term(term: &mut Term, terms: &[Term], idx: &mut usize) {
    match term {
        Term::BindMarker(_) => {
            if *idx < terms.len() {
                *term = terms[*idx].clone();
                *idx += 1;
            }
        }
        Term::InList(items)
        | Term::ListLiteral(items)
        | Term::SetLiteral(items)
        | Term::TupleLiteral(items) => {
            for item in items.iter_mut() {
                substitute_in_term(item, terms, idx);
            }
        }
        Term::MapLiteral(entries) => {
            for (k, v) in entries.iter_mut() {
                substitute_in_term(k, terms, idx);
                substitute_in_term(v, terms, idx);
            }
        }
        Term::FunctionCall { args, .. } => {
            for arg in args.iter_mut() {
                substitute_in_term(arg, terms, idx);
            }
        }
        // Literal values — nothing to substitute.
        _ => {}
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

    // -----------------------------------------------------------------------
    // substitute_bound_values tests
    // -----------------------------------------------------------------------

    fn make_plan(query: &str, bound_cols: Vec<(&str, CqlType)>) -> PreparedPlan {
        let stmt = parser::parse(query).unwrap();
        let (table_ks, table_name) = match &stmt {
            Statement::Select(s) => (s.keyspace.clone().unwrap_or_default(), s.table.clone()),
            Statement::Insert(i) => (i.keyspace.clone().unwrap_or_default(), i.table.clone()),
            _ => (String::new(), String::new()),
        };
        PreparedPlan {
            id: [0u8; 16],
            query: query.to_string(),
            statement: stmt,
            keyspace: None,
            result_columns: Vec::new(),
            bound_columns: bound_cols
                .into_iter()
                .map(|(n, t)| (n.to_string(), t))
                .collect(),
            table_keyspace: table_ks,
            table_name,
        }
    }

    /// Build a V4 EXECUTE-style value payload: [byte flags][short n_values][values...]
    fn encode_values(values: &[&[u8]]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(0x01); // flags: Values present
        buf.extend_from_slice(&(values.len() as u16).to_be_bytes()); // n_values
        for val in values {
            buf.extend_from_slice(&(val.len() as i32).to_be_bytes()); // value length
            buf.extend_from_slice(val); // value bytes
        }
        buf
    }

    #[test]
    fn substitute_uuid_values_in_select() {
        // Given: a SELECT with 2 UUID bind markers
        let plan = make_plan(
            "SELECT entity_id FROM ks.t WHERE tenant_id = ? AND session_id = ?",
            vec![("tenant_id", CqlType::Uuid), ("session_id", CqlType::Uuid)],
        );

        // When: we substitute 2 UUID values
        let uuid1 = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let uuid2 = uuid::Uuid::parse_str("86da7931-7c87-54fe-8a49-eabc21c025aa").unwrap();
        let payload = encode_values(&[uuid1.as_bytes(), uuid2.as_bytes()]);
        let result = substitute_bound_values(&plan, &payload).unwrap();

        // Then: the WHERE clause values should be UuidLiterals, not BindMarkers
        if let Statement::Select(s) = &result {
            assert_eq!(s.where_clauses.len(), 2);
            assert!(
                matches!(&s.where_clauses[0].value, Term::UuidLiteral(u) if *u == uuid1),
                "first WHERE value should be UUID1, got {:?}",
                s.where_clauses[0].value
            );
            assert!(
                matches!(&s.where_clauses[1].value, Term::UuidLiteral(u) if *u == uuid2),
                "second WHERE value should be UUID2, got {:?}",
                s.where_clauses[1].value
            );
        } else {
            panic!("expected Select statement");
        }
    }

    #[test]
    fn substitute_no_values_flag_returns_unchanged() {
        // Given: a SELECT with bind markers
        let plan = make_plan(
            "SELECT id FROM ks.t WHERE pk = ?",
            vec![("pk", CqlType::Int)],
        );

        // When: the frame has no values (flags = 0)
        let payload = vec![0x00u8]; // flags: no values
        let result = substitute_bound_values(&plan, &payload).unwrap();

        // Then: bind markers remain
        if let Statement::Select(s) = &result {
            assert!(matches!(&s.where_clauses[0].value, Term::BindMarker(_)));
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn substitute_null_value_becomes_term_null() {
        let plan = make_plan(
            "SELECT id FROM ks.t WHERE pk = ?",
            vec![("pk", CqlType::Int)],
        );

        // Null value: length = -1
        let mut payload = Vec::new();
        payload.push(0x01); // flags: Values
        payload.extend_from_slice(&1u16.to_be_bytes()); // 1 value
        payload.extend_from_slice(&(-1i32).to_be_bytes()); // null

        let result = substitute_bound_values(&plan, &payload).unwrap();
        if let Statement::Select(s) = &result {
            assert!(
                matches!(&s.where_clauses[0].value, Term::Null),
                "null value should become Term::Null"
            );
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn substitute_insert_with_mixed_types() {
        let plan = make_plan(
            "INSERT INTO ks.t (id, name, score) VALUES (?, ?, ?)",
            vec![
                ("id", CqlType::Uuid),
                ("name", CqlType::Varchar),
                ("score", CqlType::Int),
            ],
        );

        let uuid = uuid::Uuid::new_v4();
        let name = b"test_entity";
        let score = 42i32.to_be_bytes();
        let payload = encode_values(&[uuid.as_bytes(), name, &score]);

        let result = substitute_bound_values(&plan, &payload).unwrap();
        if let Statement::Insert(i) = &result {
            assert_eq!(i.values.len(), 3);
            assert!(matches!(&i.values[0], Term::UuidLiteral(_)));
            assert!(matches!(&i.values[1], Term::StringLiteral(s) if s == "test_entity"));
            assert!(matches!(&i.values[2], Term::IntegerLiteral(42)));
        } else {
            panic!("expected Insert");
        }
    }

    #[test]
    fn substitute_empty_cursor_returns_unchanged() {
        let plan = make_plan(
            "SELECT id FROM ks.t WHERE pk = ?",
            vec![("pk", CqlType::Int)],
        );

        let result = substitute_bound_values(&plan, &[]).unwrap();
        if let Statement::Select(s) = &result {
            assert!(matches!(&s.where_clauses[0].value, Term::BindMarker(_)));
        } else {
            panic!("expected Select");
        }
    }

    #[test]
    fn substitute_more_values_than_bound_cols_uses_blob() {
        // Only 1 bound column known, but 2 values sent
        let plan = make_plan(
            "SELECT id FROM ks.t WHERE pk = ? AND extra = ?",
            vec![("pk", CqlType::Int)],
        );

        let v1 = 42i32.to_be_bytes();
        let v2 = b"unknown";
        let payload = encode_values(&[&v1, v2]);

        let result = substitute_bound_values(&plan, &payload).unwrap();
        if let Statement::Select(s) = &result {
            assert!(
                matches!(&s.where_clauses[0].value, Term::IntegerLiteral(42)),
                "first value should be Int"
            );
            assert!(
                matches!(&s.where_clauses[1].value, Term::BlobLiteral(_)),
                "second value should fall back to Blob, got {:?}",
                s.where_clauses[1].value
            );
        } else {
            panic!("expected Select");
        }
    }

    // -----------------------------------------------------------------------
    // raw_bytes_to_term tests (BUG-025 / BUG-026)
    // -----------------------------------------------------------------------

    /// Build CQL v4+ wire-format bytes for a map<text,int> with one entry.
    ///
    /// Format: [4-byte BE count][4-byte BE key_len][key_bytes][4-byte BE val_len][val_bytes]
    fn encode_cql_map(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(entries.len() as i32).to_be_bytes());
        for (k, v) in entries {
            buf.extend_from_slice(&(k.len() as i32).to_be_bytes());
            buf.extend_from_slice(k);
            buf.extend_from_slice(&(v.len() as i32).to_be_bytes());
            buf.extend_from_slice(v);
        }
        buf
    }

    #[test]
    fn map_bind_value_roundtrip() {
        // Given: map<text,int> type and wire bytes for {'key': 42}
        let map_type = CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int));
        let key_bytes = b"key";
        let val_bytes = 42i32.to_be_bytes();
        let wire_bytes = encode_cql_map(&[(key_bytes, &val_bytes)]);

        // When: we call raw_bytes_to_term
        let term = raw_bytes_to_term(&map_type, &wire_bytes);

        // Then: the result must be a BlobLiteral that preserves all wire bytes,
        // NOT a truncated blob of 4 bytes (what the old fallthrough would produce).
        match &term {
            Term::BlobLiteral(b) => {
                assert_eq!(
                    b.as_slice(),
                    wire_bytes.as_slice(),
                    "map bind value must roundtrip as opaque wire bytes, got {} bytes expected {}",
                    b.len(),
                    wire_bytes.len()
                );
            }
            other => panic!(
                "map bind value should be BlobLiteral for storage passthrough, got {:?}",
                other
            ),
        }
    }

    // -----------------------------------------------------------------------
    // BUG-021 regression tests — bind values in QUERY frames must not be
    // silently ignored. Each test exercises substitute_bound_values() for one
    // DML statement type so any future regression is caught immediately.
    // -----------------------------------------------------------------------

    /// BUG-021 / bind_values_select: a SELECT with WHERE pk = ? and a VARCHAR
    /// bind value must produce a StringLiteral in the WHERE clause, not a
    /// BindMarker.
    #[test]
    fn bind_values_select() {
        let plan = make_plan(
            "SELECT v FROM ks.t WHERE pk = ?",
            vec![("pk", CqlType::Varchar)],
        );

        let pk_val = b"partition-key-1";
        let payload = encode_values(&[pk_val]);

        let result = substitute_bound_values(&plan, &payload).unwrap();

        if let Statement::Select(s) = &result {
            assert_eq!(s.where_clauses.len(), 1, "expected one WHERE clause");
            assert!(
                matches!(&s.where_clauses[0].value, Term::StringLiteral(v) if v == "partition-key-1"),
                "BUG-021: WHERE value should be StringLiteral(\"partition-key-1\"), got {:?}",
                s.where_clauses[0].value
            );
        } else {
            panic!("expected Select statement");
        }
    }

    /// BUG-021 / bind_values_insert: an INSERT with VALUES (?, ?) and two bind
    /// values must substitute both placeholders. A BindMarker in the result
    /// means the values were silently ignored.
    #[test]
    fn bind_values_insert() {
        let plan = make_plan(
            "INSERT INTO ks.t (pk, v) VALUES (?, ?)",
            vec![("pk", CqlType::Int), ("v", CqlType::Varchar)],
        );

        let pk_val = 7i32.to_be_bytes();
        let v_val = b"hello";
        let payload = encode_values(&[&pk_val, v_val]);

        let result = substitute_bound_values(&plan, &payload).unwrap();

        if let Statement::Insert(i) = &result {
            assert_eq!(i.values.len(), 2, "expected two INSERT values");
            assert!(
                matches!(&i.values[0], Term::IntegerLiteral(7)),
                "BUG-021: first value should be IntegerLiteral(7), got {:?}",
                i.values[0]
            );
            assert!(
                matches!(&i.values[1], Term::StringLiteral(s) if s == "hello"),
                "BUG-021: second value should be StringLiteral(\"hello\"), got {:?}",
                i.values[1]
            );
        } else {
            panic!("expected Insert statement");
        }
    }

    /// BUG-021 / bind_values_update: an UPDATE with SET v = ? WHERE pk = ? and
    /// two bind values must substitute both — the assignment and the WHERE
    /// predicate.
    #[test]
    fn bind_values_update() {
        let plan = make_plan(
            "UPDATE ks.t SET v = ? WHERE pk = ?",
            vec![("v", CqlType::Varchar), ("pk", CqlType::Int)],
        );

        let v_val = b"updated";
        let pk_val = 42i32.to_be_bytes();
        let payload = encode_values(&[v_val, &pk_val]);

        let result = substitute_bound_values(&plan, &payload).unwrap();

        if let Statement::Update(u) = &result {
            assert_eq!(u.assignments.len(), 1, "expected one assignment");
            assert_eq!(u.where_clauses.len(), 1, "expected one WHERE clause");

            // Assignment value must be substituted.
            if let Assignment::Simple { value, .. } = &u.assignments[0] {
                assert!(
                    matches!(value, Term::StringLiteral(s) if s == "updated"),
                    "BUG-021: assignment value should be StringLiteral(\"updated\"), got {:?}",
                    value
                );
            } else {
                panic!("expected Simple assignment");
            }

            // WHERE clause value must be substituted.
            assert!(
                matches!(&u.where_clauses[0].value, Term::IntegerLiteral(42)),
                "BUG-021: WHERE value should be IntegerLiteral(42), got {:?}",
                u.where_clauses[0].value
            );
        } else {
            panic!("expected Update statement");
        }
    }

    /// BUG-021 / bind_values_delete: a DELETE with WHERE pk = ? and a bind
    /// value must substitute the placeholder so the delete targets the correct
    /// partition.
    #[test]
    fn bind_values_delete() {
        let plan = make_plan("DELETE FROM ks.t WHERE pk = ?", vec![("pk", CqlType::Int)]);

        let pk_val = 99i32.to_be_bytes();
        let payload = encode_values(&[&pk_val]);

        let result = substitute_bound_values(&plan, &payload).unwrap();

        if let Statement::Delete(d) = &result {
            assert_eq!(d.where_clauses.len(), 1, "expected one WHERE clause");
            assert!(
                matches!(&d.where_clauses[0].value, Term::IntegerLiteral(99)),
                "BUG-021: WHERE value should be IntegerLiteral(99), got {:?}",
                d.where_clauses[0].value
            );
        } else {
            panic!("expected Delete statement");
        }
    }

    /// Exercise all 10 CQL scalar types as bind values in a single INSERT.
    ///
    /// Each of the 10 columns maps to one wire-format encoding:
    /// text → UTF-8, int → 4B BE, bigint → 8B BE, boolean → 1B,
    /// uuid → 16B, timestamp → 8B BE, float → 4B BE, double → 8B BE,
    /// blob → raw bytes, inet → 4B IPv4.
    #[test]
    fn bind_values_ten_types() {
        let plan = make_plan(
            "INSERT INTO ks.t (c_text, c_int, c_bigint, c_bool, c_uuid, \
             c_ts, c_float, c_double, c_blob, c_inet) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            vec![
                ("c_text", CqlType::Varchar),
                ("c_int", CqlType::Int),
                ("c_bigint", CqlType::Bigint),
                ("c_bool", CqlType::Boolean),
                ("c_uuid", CqlType::Uuid),
                ("c_ts", CqlType::Timestamp),
                ("c_float", CqlType::Float),
                ("c_double", CqlType::Double),
                ("c_blob", CqlType::Blob),
                ("c_inet", CqlType::Inet),
            ],
        );

        let text_val = b"hello";
        let int_val = 42i32.to_be_bytes();
        let bigint_val = 9_000_000_000i64.to_be_bytes();
        let bool_val = [1u8];
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let test_uuid = uuid::Uuid::parse_str(uuid_str).unwrap();
        let uuid_val = *test_uuid.as_bytes();
        let ts_val = 1_700_000_000_000i64.to_be_bytes();
        let float_val = 3.25f32.to_bits().to_be_bytes();
        let double_val = 2.75f64.to_bits().to_be_bytes();
        let blob_val = [0xDE, 0xAD, 0xBE, 0xEF];
        let inet_val = [192u8, 168, 1, 1];

        let payload = encode_values(&[
            text_val,
            &int_val,
            &bigint_val,
            &bool_val,
            &uuid_val,
            &ts_val,
            &float_val,
            &double_val,
            &blob_val,
            &inet_val,
        ]);

        let result = substitute_bound_values(&plan, &payload).unwrap();

        if let Statement::Insert(i) = &result {
            assert_eq!(i.values.len(), 10, "expected 10 INSERT values");

            assert!(
                matches!(&i.values[0], Term::StringLiteral(s) if s == "hello"),
                "c_text should be StringLiteral(\"hello\"), got {:?}",
                i.values[0]
            );
            assert!(
                matches!(&i.values[1], Term::IntegerLiteral(42)),
                "c_int should be IntegerLiteral(42), got {:?}",
                i.values[1]
            );
            assert!(
                matches!(&i.values[2], Term::IntegerLiteral(9_000_000_000)),
                "c_bigint should be IntegerLiteral(9000000000), got {:?}",
                i.values[2]
            );
            assert!(
                matches!(&i.values[3], Term::BoolLiteral(true)),
                "c_bool should be BoolLiteral(true), got {:?}",
                i.values[3]
            );
            assert!(
                matches!(&i.values[4], Term::UuidLiteral(u) if *u == test_uuid),
                "c_uuid should be UuidLiteral({uuid_str}), got {:?}",
                i.values[4]
            );
            assert!(
                matches!(&i.values[5], Term::IntegerLiteral(1_700_000_000_000)),
                "c_ts should be IntegerLiteral(1700000000000), got {:?}",
                i.values[5]
            );
            // Float decodes to f64 via f32::from_bits → as f64
            let expected_float = 3.25f32 as f64;
            assert!(
                matches!(&i.values[6], Term::FloatLiteral(f) if (*f - expected_float).abs() < 1e-6),
                "c_float should be FloatLiteral(~3.25), got {:?}",
                i.values[6]
            );
            let expected_double = 2.75f64;
            assert!(
                matches!(&i.values[7], Term::FloatLiteral(f) if (*f - expected_double).abs() < 1e-9),
                "c_double should be FloatLiteral(~2.75), got {:?}",
                i.values[7]
            );
            assert!(
                matches!(&i.values[8], Term::BlobLiteral(b) if b.as_slice() == [0xDE, 0xAD, 0xBE, 0xEF]),
                "c_blob should be BlobLiteral([DE AD BE EF]), got {:?}",
                i.values[8]
            );
            // Inet 192.168.1.1 → StringLiteral("192.168.1.1")
            assert!(
                matches!(&i.values[9], Term::StringLiteral(s) if s == "192.168.1.1"),
                "c_inet should be StringLiteral(\"192.168.1.1\"), got {:?}",
                i.values[9]
            );
        } else {
            panic!("expected Insert statement");
        }
    }

    /// A 4-byte blob value bound to a map<text,bigint> column must NOT be
    /// misinterpreted as a partial map — it must round-trip as a BlobLiteral
    /// containing all 4 bytes.
    ///
    /// This is the Cassandra-compat case: the driver has already encoded the map
    /// in wire format; we must store it verbatim for the storage layer to decode.
    #[test]
    fn map_bind_value_cassandra_compat() {
        // Given: map<text,bigint> type and a tiny 4-byte blob that could be
        // misread as a 4-byte "count" field if the code mistakenly parses it.
        let map_type = CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Bigint));
        let tiny_blob: &[u8] = &[0x00, 0x00, 0x00, 0x01];

        // When: we call raw_bytes_to_term with the Map type annotation
        let term = raw_bytes_to_term(&map_type, tiny_blob);

        // Then: the result must be a BlobLiteral containing ALL 4 bytes, not a
        // truncated blob or an error — map bytes are passed through unchanged.
        match &term {
            Term::BlobLiteral(b) => {
                assert_eq!(
                    b.as_slice(),
                    tiny_blob,
                    "4-byte map bind value must be preserved as-is, \
                     got {} bytes: {:?}",
                    b.len(),
                    b
                );
            }
            other => panic!(
                "map<text,bigint> bind value should be BlobLiteral, got {:?}",
                other
            ),
        }
    }
}
