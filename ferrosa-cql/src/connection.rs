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

use crate::ast::{Assignment, SelectColumn, SelectStatement, Statement, Term};
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
use crate::types::{decode_value, encode_value, CqlType, CqlValue};
use crate::virtual_tables::connections::{ConnectionInfo, ConnectionTracker};

use ferrosa_schema::AuthContext;

use futures::SinkExt;

/// Idle timeout: drop connection if no complete frame arrives within this duration (M11).
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Authenticate on the blocking pool so bcrypt (`cost=12` ≈ 200 ms per call on
/// commodity hardware) does not block the async worker that runs this
/// connection's handler. Without offloading, a burst of concurrent auth
/// attempts pins every async worker thread in CPU-bound `bcrypt::verify`
/// for hundreds of milliseconds at a time, starving the accept loop and
/// every other connection on the same runtime.
///
/// The `JoinError` path (task panicked) is mapped to `AuthenticationFailed`
/// so panics in the auth path become auth failures rather than dropped
/// futures.
pub(crate) async fn authenticate_off_runtime(
    schema: Arc<ferrosa_schema::Schema>,
    username: String,
    password: String,
) -> ferrosa_schema::Result<ferrosa_schema::AuthContext> {
    authenticate_off_runtime_observed(schema, username, password, || {}).await
}

async fn authenticate_off_runtime_observed<F>(
    schema: Arc<ferrosa_schema::Schema>,
    username: String,
    password: String,
    on_blocking_start: F,
) -> ferrosa_schema::Result<ferrosa_schema::AuthContext>
where
    F: FnOnce() + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        on_blocking_start();
        schema.authenticate(&username, &password)
    })
    .await
    .unwrap_or(Err(ferrosa_schema::SchemaError::AuthenticationFailed))
}

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
pub(crate) async fn handle_connection<S>(
    stream: S,
    peer: SocketAddr,
    max_frame_size: u32,
    max_in_flight: usize,
    auth_disabled: bool,
    state: Arc<SharedState>,
    mut ip_slot: Option<crate::server::IpSlotGuard>,
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
    let mut client_use_beta: bool = false; // USE_BETA flag (0x10) in STARTUP

    // Channel for subscription tasks to push streaming frames.
    let (sub_tx, mut sub_rx) = tokio::sync::mpsc::channel::<crate::subscribe::SubscriptionPush>(64);

    // In-flight request limiter: bounds concurrent requests on this connection.
    // `Arc<Semaphore>` so spawned per-request tasks can hold owned permits;
    // see the `dispatch_concurrent` path below.
    let in_flight = Arc::new(tokio::sync::Semaphore::new(max_in_flight));

    // Channel for responses produced by spawned request handlers (Query /
    // Execute / Batch) back to this main loop, which owns `framed`. Without
    // this, every request had to be fully processed before the loop could
    // even read the next frame — turning CQL's stream-id-multiplexed protocol
    // into a strictly-serial one-at-a-time channel and capping throughput
    // at `1 / per_request_latency` per connection regardless of how many
    // in-flight permits we advertised.
    let (resp_tx, mut resp_rx) =
        tokio::sync::mpsc::channel::<SpawnedResponse>(max_in_flight.max(64));

    loop {
        // Use select to handle client frames, subscription pushes, and
        // responses from spawned request handlers.
        //
        // `biased;` prefers draining responses first so the writer side
        // doesn't lag the reader side under heavy concurrent load.
        let frame_or_push = tokio::select! {
            biased;
            Some(resp) = resp_rx.recv() => FrameOrPush::Response(resp),
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
            FrameOrPush::Response(resp) => {
                // Apply any keyspace mutation that the spawned handler
                // observed (e.g. a `USE` statement). Mutations land here in
                // spawn-completion order, not request-arrival order — same
                // semantics as Cassandra under concurrent USE issuance.
                if let Some(ks) = resp.keyspace_after {
                    current_keyspace = ks;
                }
                if resp.bump_request_counter {
                    state.connection_tracker.increment_requests(&peer);
                }
                if !apply_handle_result(
                    resp.result,
                    resp.stream_id,
                    resp.response_version,
                    &mut framed,
                    &state,
                    peer,
                    &auth_context,
                    &current_keyspace,
                    &sub_tx,
                    &mut subscription_state,
                )
                .await
                {
                    break;
                }
            }
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
                // The permit lifetime is tied to the request: held inline
                // (in `_permit`) for the rare opcodes still handled inline,
                // and *moved into the spawned task* for Ready-phase
                // Query/Execute/Batch via the concurrent dispatch path.
                let is_request = matches!(
                    maybe_frame.header.opcode,
                    Opcode::Query | Opcode::Execute | Opcode::Batch
                );
                let acquired_permit: Option<tokio::sync::OwnedSemaphorePermit> = if is_request {
                    match in_flight.clone().try_acquire_owned() {
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

                // Track the client's protocol version and USE_BETA flag
                // from the STARTUP frame.
                if matches!(phase, ConnectionPhase::AwaitingStartup) {
                    client_protocol_version = maybe_frame.header.version;
                    client_use_beta = maybe_frame.header.flags & 0x10 != 0;
                }
                let was_awaiting_startup = matches!(phase, ConnectionPhase::AwaitingStartup);
                let was_ready = matches!(phase, ConnectionPhase::Ready);

                // Concurrent dispatch path: in `Ready` phase, run the
                // common request opcodes on spawned tasks so the main
                // loop can immediately read the next frame. This is what
                // turns a per-connection serial channel into a real
                // stream-id-multiplexed one (matching CQL native
                // protocol semantics).
                if was_ready
                    && matches!(
                        maybe_frame.header.opcode,
                        Opcode::Query | Opcode::Execute | Opcode::Batch
                    )
                {
                    let permit =
                        acquired_permit.expect("permit acquired above for Query/Execute/Batch");
                    let response_version = if client_protocol_version >= 0x05 {
                        0x85
                    } else {
                        VERSION_RESPONSE
                    };
                    let opcode = maybe_frame.header.opcode;
                    let proto_version = client_protocol_version;
                    let body = maybe_frame.body.clone();
                    let state_clone = state.clone();
                    let resp_tx = resp_tx.clone();
                    let ks_at_spawn = current_keyspace.clone();
                    let mut auth_for_handler = auth_context.clone();
                    let mut ks_for_handler = ks_at_spawn.clone();
                    let peer_addr = peer;
                    tokio::spawn(async move {
                        let request_span = tracing::info_span!(
                            "cql.request",
                            cql.opcode = ?opcode,
                            client.address = %peer_addr,
                        );
                        let result = (async {
                            match opcode {
                                Opcode::Query => {
                                    handle_query(
                                        &mut auth_for_handler,
                                        &mut ks_for_handler,
                                        &state_clone,
                                        &body,
                                        peer_addr,
                                        proto_version,
                                    )
                                    .await
                                }
                                Opcode::Execute => {
                                    handle_execute(
                                        &mut auth_for_handler,
                                        &mut ks_for_handler,
                                        &state_clone,
                                        &body,
                                        peer_addr,
                                        proto_version,
                                    )
                                    .await
                                }
                                Opcode::Batch => {
                                    handle_batch(
                                        &mut auth_for_handler,
                                        &mut ks_for_handler,
                                        &state_clone,
                                        &body,
                                        peer_addr,
                                    )
                                    .await
                                }
                                _ => unreachable!(
                                    "concurrent dispatch only entered for Query/Execute/Batch"
                                ),
                            }
                        })
                        .instrument(request_span)
                        .await;
                        let keyspace_after = if ks_for_handler != ks_at_spawn {
                            Some(ks_for_handler)
                        } else {
                            None
                        };
                        let _ = resp_tx
                            .send(SpawnedResponse {
                                stream_id,
                                result,
                                response_version,
                                keyspace_after,
                                bump_request_counter: true,
                                _permit: permit,
                            })
                            .await;
                    });
                    continue;
                }

                // Inline path: handshake phases and rare Ready opcodes
                // (Prepare, Register, Options). Keeps mutable connection
                // state (`phase`, `auth_context`, `pending_compression`)
                // owned by the main loop.
                let _permit = acquired_permit;

                debug!(
                    "received {:?} from {peer} stream={} phase={:?}",
                    maybe_frame.header.opcode, stream_id, phase
                );

                let request_span = tracing::info_span!(
                    "cql.request",
                    cql.opcode = ?maybe_frame.header.opcode,
                    client.address = %peer,
                );

                match (async {
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
                })
                .instrument(request_span)
                .await
                {
                    HandleResult::Reply(opcode, body) => {
                        debug!(
                            "replying {:?} to {peer} stream={} phase={:?}",
                            opcode, stream_id, phase
                        );
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
                            // F3: drop the per-IP rate-limit slot the instant
                            // the connection reaches Ready. The per-IP cap is a
                            // defence against unauthenticated connection storms;
                            // once a client has completed the handshake (and auth,
                            // if enabled), it should not be counted toward that
                            // cap — otherwise a burst from one IP holds slots
                            // for the full IDLE_TIMEOUT even after all clients
                            // succeed, rejecting legitimate follow-up traffic.
                            ip_slot.take();
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
                            // Stay on legacy (unframed) envelope transport for
                            // all clients. Real drivers that negotiate v5 —
                            // including ones that set USE_BETA (0x10), like
                            // gocql and the DataStax Java driver — send plain
                            // 9-byte envelopes, NOT CRC24/CRC32 modern frames.
                            // Switching to modern framing on USE_BETA misreads
                            // their legacy envelopes (a v5 frame-header CRC24
                            // mismatch on every request). v5 message semantics
                            // ride over the legacy transport.
                            let _ = client_use_beta;
                        }
                    }
                    HandleResult::StartSubscription {
                        inner,
                        interval,
                        delta,
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
                            delta,
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

/// Apply a `HandleResult` from a spawned post-Ready handler to the
/// connection's outbound side: writes the appropriate frame on `framed`,
/// kicks off / cancels subscriptions if requested. Returns `false` if the
/// caller should break out of the connection loop (e.g. socket send
/// failure or `HandleResult::Close*`).
///
/// Only handles cases reachable from `Query` / `Execute` / `Batch` —
/// handshake-driven codec changes (compression, v5-framing) stay on the
/// inline path because they only fire for `Opcode::Ready` /
/// `Opcode::AuthSuccess` and the inline match has the
/// `client_protocol_version` / `pending_compression` it needs.
#[allow(clippy::too_many_arguments)]
async fn apply_handle_result<S>(
    result: HandleResult,
    stream_id: i16,
    response_version: u8,
    framed: &mut Framed<S, CqlCodec>,
    state: &Arc<SharedState>,
    _peer: SocketAddr,
    auth_context: &Option<AuthContext>,
    current_keyspace: &Option<String>,
    sub_tx: &tokio::sync::mpsc::Sender<crate::subscribe::SubscriptionPush>,
    subscription_state: &mut SubscriptionState,
) -> bool
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    match result {
        HandleResult::Reply(opcode, body) => {
            let frame = CqlFrame {
                header: FrameHeader {
                    version: response_version,
                    flags: 0,
                    stream_id,
                    opcode,
                    length: 0,
                },
                body: body.freeze(),
            };
            framed.send(frame).await.is_ok()
        }
        HandleResult::StartSubscription {
            inner,
            interval,
            delta,
        } => {
            let interval = match interval {
                Some(d) => d,
                None => {
                    let err = CqlError::Invalid(
                        "SUBSCRIBE without EVERY not yet supported; use SUBSCRIBE ... EVERY <interval>"
                            .into(),
                    );
                    let frame = CqlFrame {
                        header: FrameHeader {
                            version: VERSION_RESPONSE,
                            flags: 0,
                            stream_id,
                            opcode: Opcode::Error,
                            length: 0,
                        },
                        body: err.encode_body().freeze(),
                    };
                    return framed.send(frame).await.is_ok();
                }
            };
            let cancel = tokio_util::sync::CancellationToken::new();
            let handle = crate::subscribe::SubscriptionHandle {
                stream_id: stream_id as u16,
                cancel: cancel.clone(),
            };
            if let Err(msg) = subscription_state.add(handle) {
                let err = CqlError::Invalid(msg.to_string());
                let frame = CqlFrame {
                    header: FrameHeader {
                        version: VERSION_RESPONSE,
                        flags: 0,
                        stream_id,
                        opcode: Opcode::Error,
                        length: 0,
                    },
                    body: err.encode_body().freeze(),
                };
                return framed.send(frame).await.is_ok();
            }
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
                return false;
            }
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
                delta,
            );
            true
        }
        HandleResult::CancelSubscription { stream_id: sub_id } => {
            subscription_state.cancel(sub_id);
            let frame = CqlFrame {
                header: FrameHeader {
                    version: VERSION_RESPONSE,
                    flags: 0,
                    stream_id,
                    opcode: Opcode::Result,
                    length: 0,
                },
                body: crate::result::encode_void().freeze(),
            };
            framed.send(frame).await.is_ok()
        }
        HandleResult::Close(opcode, body) => {
            let frame = CqlFrame {
                header: FrameHeader {
                    version: VERSION_RESPONSE,
                    flags: 0,
                    stream_id,
                    opcode,
                    length: 0,
                },
                body: body.freeze(),
            };
            let _ = framed.send(frame).await;
            false
        }
        HandleResult::CloseNow => false,
    }
}

/// Internal enum for the select! loop — either a client frame, a subscription
/// push, or a response from a spawned request handler.
enum FrameOrPush {
    ClientFrame(CqlFrame),
    SubscriptionPush(crate::subscribe::SubscriptionPush),
    Response(SpawnedResponse),
}

/// Response produced by a spawned request handler, carrying back to the
/// main loop everything it needs to (1) update connection-local state and
/// (2) write the response frame.
pub(crate) struct SpawnedResponse {
    /// Stream id of the original request frame — echoed in the response
    /// so the client can correlate.
    stream_id: i16,
    /// Whatever the handler produced; the main loop matches on this just
    /// like the inline path.
    result: HandleResult,
    /// Response frame version (`0x84` for v4, `0x85` for v5). Captured at
    /// dispatch time because `client_protocol_version` is per-connection
    /// state owned by the main loop.
    response_version: u8,
    /// New keyspace value if the handler observed a `USE` statement.
    /// `None` means unchanged; `Some(x)` is the new value.
    keyspace_after: Option<Option<String>>,
    /// `true` for spawned post-Ready requests so the main loop bumps the
    /// per-connection request counter — matching the inline path's
    /// `was_ready` bookkeeping.
    bump_request_counter: bool,
    /// Held until the handler responds so the in-flight permit is only
    /// released after the response is buffered on the channel.
    _permit: tokio::sync::OwnedSemaphorePermit,
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
            Opcode::AuthResponse => {
                handle_auth_response(phase, auth_context, state, &frame.body).await
            }
            _ => {
                // Any opcode other than AUTH_RESPONSE before authentication is
                // complete is an unauthorized access attempt — return 0x2100, not
                // a Protocol error (0x000A).  This matches the intent of the CQL
                // spec: the client is not authenticated, so they are unauthorized.
                let err = CqlError::Unauthorized(format!(
                    "authentication required before sending {:?}",
                    frame.header.opcode
                ));
                HandleResult::Reply(Opcode::Error, err.encode_body())
            }
        },
        ConnectionPhase::Ready => match frame.header.opcode {
            Opcode::Query => {
                handle_query(
                    auth_context,
                    current_keyspace,
                    state,
                    &frame.body,
                    peer,
                    frame.header.version,
                )
                .await
            }
            Opcode::Prepare => handle_prepare(auth_context, current_keyspace, state, &frame.body),
            Opcode::Execute => {
                handle_execute(
                    auth_context,
                    current_keyspace,
                    state,
                    &frame.body,
                    peer,
                    frame.header.version,
                )
                .await
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

    debug!(
        "startup options: cql_version={:?} compression={:?} auth_disabled={}",
        cql_version, compression_name, auth_disabled
    );

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

async fn handle_auth_response(
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

    let (username, password) = match parse_sasl_plain(payload) {
        Ok(creds) => creds,
        Err(_) => {
            let err = CqlError::BadCredentials;
            return increment_auth_attempts_and_reply(phase, err);
        }
    };

    // Offload bcrypt to the blocking pool — see `authenticate_off_runtime`.
    let result = authenticate_off_runtime(
        state.schema.clone(),
        username.to_string(),
        password.to_string(),
    )
    .await;

    match result {
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
    protocol_version: u8,
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
        stmt = match substitute_bound_values(&temp_plan, cursor, protocol_version) {
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

    // Count bind markers in the AST before touching the schema.  This count is
    // the ground truth for how many col_specs the PREPARE response must carry.
    // Strict external CQL drivers (scylla, gocql, DataStax Java/C#) reject
    // `execute_unpaged` when the driver's cached col_count from the PREPARE
    // response does not match the number of Rust values in the call:
    //   WrongColumnCount { rust_cols: N, cql_cols: <what we reported> }
    let expected_bind_count = count_bind_markers(&stmt);

    // Build bound_columns and result_columns from the statement + schema metadata.
    let (bound_columns, result_columns) =
        analyze_prepared_columns(&stmt, &table_ks, &table_name, state);

    // Guard: if the schema lookup returned fewer bound columns than the
    // statement has bind markers, something is wrong with this node's local
    // schema snapshot.  The most common cause is Raft replication lag: the
    // CREATE TABLE entry has been committed on the leader and returned to the
    // client, but has not yet been applied to this follower's state machine.
    //
    // Returning a PREPARED response with col_count=0 (or any count ≠ N) would
    // be a silent protocol violation that only strict drivers catch — and they
    // catch it as a non-retriable execute error, not as a PREPARE error.
    //
    // The correct behavior is to return an error here so the driver retries
    // PREPARE (potentially on a different node where the schema is current).
    // Per the CQL native protocol spec, an `Invalid` error on PREPARE is
    // retriable.  Real Cassandra returns 0x2200 (Invalid) when the table is
    // not found during PREPARE.
    if bound_columns.len() != expected_bind_count {
        let err = CqlError::Invalid(format!(
            "PREPARE failed for '{query}': \
             expected {expected_bind_count} bind-marker column spec(s) \
             but resolved only {}. \
             Table '{table_ks}.{table_name}' may not yet be visible on this node \
             (schema replication lag). Retry in a moment.",
            bound_columns.len()
        ));
        return HandleResult::Reply(Opcode::Error, err.encode_body());
    }

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
    protocol_version: u8,
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
    let stmt = match substitute_bound_values(&plan, cursor, protocol_version) {
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

        // Parse the statement, then substitute its bound values. BATCH carries
        // per-statement values (`[short n_values]([int len][bytes])*`) right
        // after each statement — they must be bound into the bind markers, not
        // skipped, or route() rejects the leftover markers.
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

            let parsed = match parser::parse(&query) {
                Ok(s) => s,
                Err(e) => {
                    return HandleResult::Reply(Opcode::Error, e.encode_body());
                }
            };

            // Resolve bind-marker types from the schema (same as the
            // unprepared QUERY-with-values path) so values decode correctly.
            let (table_ks, table_name) = extract_keyspace_table(&parsed, current_keyspace);
            let (bound_columns, _) =
                analyze_prepared_columns(&parsed, &table_ks, &table_name, state);
            let temp_plan = PreparedPlan {
                id: [0u8; 16],
                query,
                statement: parsed,
                keyspace: current_keyspace.clone(),
                result_columns: Vec::new(),
                bound_columns,
                table_keyspace: table_ks,
                table_name,
            };
            match substitute_batch_values(&temp_plan, &mut cursor) {
                Ok(s) => s,
                Err(e) => return HandleResult::Reply(Opcode::Error, e.encode_body()),
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
                Some(p) => match substitute_batch_values(&p, &mut cursor) {
                    Ok(s) => s,
                    Err(e) => return HandleResult::Reply(Opcode::Error, e.encode_body()),
                },
                None => {
                    let err = CqlError::Unprepared(id);
                    return HandleResult::Reply(Opcode::Error, err.encode_body());
                }
            }
        } else {
            let err = CqlError::Protocol(format!("BATCH: invalid statement kind {kind}"));
            return HandleResult::Reply(Opcode::Error, err.encode_body());
        };

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
    let table_meta = snap
        .tables
        .get(&(table_ks.to_string(), table_name.to_string()));

    let resolve = |col_name: &str| -> Option<CqlType> {
        table_meta
            .and_then(|table_meta| table_meta.columns.get(col_name))
            .and_then(|col| {
                bridge::parse_cql_type_in_keyspace(&col.column_type, table_ks, &state.schema).ok()
            })
            .or_else(|| system_schema_column_type(table_ks, table_name, col_name))
    };

    let mut bound_columns = Vec::new();

    match stmt {
        Statement::Select(s) => {
            // Bind markers in SELECT clauses, preserving statement bind order:
            // WHERE predicates first, then ANN vector term if `ANN OF ?` is used.
            for col_name in select_bind_marker_columns(s) {
                if let Some(cql_type) = resolve(col_name) {
                    bound_columns.push((col_name.to_string(), cql_type));
                }
            }

            // Build result columns from the SELECT column list
            let result_columns =
                build_result_columns(&s.columns, table_meta, table_ks, table_name, state);
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
            // USING TIMESTAMP ? / USING TTL ?  — Gap 10.  CQL syntactic
            // order: VALUES first, then USING.
            if matches!(i.using_timestamp, Some(Term::BindMarker(_))) {
                bound_columns.push(("[timestamp]".into(), CqlType::Bigint));
            }
            if matches!(i.using_ttl, Some(Term::BindMarker(_))) {
                bound_columns.push(("[ttl]".into(), CqlType::Int));
            }
        }
        Statement::Update(u) => {
            // UPDATE syntactic order: USING TIMESTAMP/TTL, then SET, then WHERE, then IF.
            if matches!(u.using_timestamp, Some(Term::BindMarker(_))) {
                bound_columns.push(("[timestamp]".into(), CqlType::Bigint));
            }
            if matches!(u.using_ttl, Some(Term::BindMarker(_))) {
                bound_columns.push(("[ttl]".into(), CqlType::Int));
            }
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
            // DELETE syntactic order: USING TIMESTAMP, then WHERE, then IF.
            if matches!(d.using_timestamp, Some(Term::BindMarker(_))) {
                bound_columns.push(("[timestamp]".into(), CqlType::Bigint));
            }
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

/// Return SELECT column names whose terms are bind markers, in the same order
/// drivers bind values for PREPARE/EXECUTE metadata.
///
/// `ORDER BY <vector_col> ANN OF ?` binds a query vector against the vector
/// column, so PREPARE metadata must include `<vector_col>` after ordinary
/// WHERE bind markers. Without this, `count_bind_markers()` sees the ANN
/// placeholder but `analyze_prepared_columns()` resolves one fewer column spec,
/// causing strict drivers to reject the prepared statement.
fn select_bind_marker_columns(s: &SelectStatement) -> Vec<&str> {
    let mut columns = Vec::new();

    for wc in &s.where_clauses {
        if matches!(wc.value, Term::BindMarker(_)) {
            columns.push(wc.column.as_str());
        }
    }

    if let Some((ann_column, Term::BindMarker(_))) = &s.ann_of {
        columns.push(ann_column.as_str());
    }

    columns
}

fn system_schema_column_type(table_ks: &str, table_name: &str, col_name: &str) -> Option<CqlType> {
    system_schema_column_specs(table_ks, table_name)
        .into_iter()
        .find(|(name, _)| name == col_name)
        .map(|(_, ty)| ty)
}

fn system_schema_column_specs(table_ks: &str, table_name: &str) -> Vec<(String, CqlType)> {
    if table_ks != "system_schema" {
        return Vec::new();
    }

    let text = || CqlType::Varchar;
    let text_map = || CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Varchar));
    let text_list = || CqlType::List(Box::new(CqlType::Varchar));

    let specs: Vec<(&str, CqlType)> = match table_name {
        "keyspaces" => vec![
            ("keyspace_name", text()),
            ("durable_writes", CqlType::Boolean),
            ("replication", text_map()),
        ],
        "tables" => vec![
            ("keyspace_name", text()),
            ("table_name", text()),
            ("id", CqlType::Uuid),
            ("cdc", CqlType::Boolean),
            ("allow_auto_snapshot", CqlType::Boolean),
            ("incremental_backups", CqlType::Boolean),
        ],
        "columns" => vec![
            ("keyspace_name", text()),
            ("table_name", text()),
            ("column_name", text()),
            ("kind", text()),
            ("position", CqlType::Int),
            ("type", text()),
            ("clustering_order", text()),
        ],
        "types" => vec![
            ("keyspace_name", text()),
            ("type_name", text()),
            ("field_names", text_list()),
            ("field_types", text_list()),
        ],
        "indexes" => vec![
            ("keyspace_name", text()),
            ("table_name", text()),
            ("index_name", text()),
            ("kind", text()),
            ("options", text_map()),
        ],
        "views" => vec![
            ("keyspace_name", text()),
            ("view_name", text()),
            ("base_table_id", CqlType::Uuid),
            ("base_table_name", text()),
            ("cdc", CqlType::Boolean),
            ("include_all_columns", CqlType::Boolean),
            ("allow_auto_snapshot", CqlType::Boolean),
            ("incremental_backups", CqlType::Boolean),
            ("id", CqlType::Uuid),
            ("where_clause", text()),
        ],
        "functions" => vec![
            ("keyspace_name", text()),
            ("function_name", text()),
            ("argument_types", text_list()),
            ("argument_names", text_list()),
            ("body", text()),
            ("called_on_null_input", CqlType::Boolean),
            ("language", text()),
            ("return_type", text()),
        ],
        "aggregates" => vec![
            ("keyspace_name", text()),
            ("aggregate_name", text()),
            ("argument_types", text_list()),
            ("final_func", text()),
            ("initcond", text()),
            ("return_type", text()),
            ("state_func", text()),
            ("state_type", text()),
        ],
        "triggers" => vec![
            ("keyspace_name", text()),
            ("table_name", text()),
            ("trigger_name", text()),
            ("options", text_map()),
        ],
        _ => Vec::new(),
    };

    specs
        .into_iter()
        .map(|(name, ty)| (name.to_string(), ty))
        .collect()
}

/// Count the number of `?` (positional) and `:name` (named) bind markers in
/// a parsed statement, purely from the AST — no schema lookup needed.
///
/// This count is used as a guard in `handle_prepare`: if the schema lookup
/// succeeds but `bound_columns.len()` is less than the bind-marker count, it
/// indicates that one or more columns could not be resolved (e.g., because the
/// table schema has not yet propagated to this node via Raft replication).
/// Returning `col_count = 0` (or a partial count) in the PREPARE response
/// causes strict CQL drivers (scylla, gocql, DataStax) to reject every
/// subsequent execute call with:
///   WrongColumnCount { rust_cols: N, cql_cols: 0 }
///
/// When the counts diverge, `handle_prepare` returns an `Invalid` error so
/// the driver retries on a different node where the schema is current.
fn count_bind_markers(stmt: &Statement) -> usize {
    let is_bind = |t: &Term| matches!(t, Term::BindMarker(_));

    match stmt {
        Statement::Select(s) => {
            let where_count = s
                .where_clauses
                .iter()
                .filter(|wc| is_bind(&wc.value))
                .count();
            let ann_count = s
                .ann_of
                .as_ref()
                .filter(|(_, t)| is_bind(t))
                .map_or(0, |_| 1);
            let limit_count = match &s.limit {
                Some(crate::ast::Limit::BindMarker)
                | Some(crate::ast::Limit::NamedBindMarker(_)) => 1,
                _ => 0,
            };
            where_count + ann_count + limit_count
        }
        Statement::Insert(i) => {
            let values = i.values.iter().filter(|v| is_bind(v)).count();
            let ts = i
                .using_timestamp
                .as_ref()
                .filter(|t| is_bind(t))
                .map_or(0, |_| 1);
            let ttl = i.using_ttl.as_ref().filter(|t| is_bind(t)).map_or(0, |_| 1);
            values + ts + ttl
        }
        Statement::Update(u) => {
            // UPDATE syntactic order: USING TIMESTAMP/TTL, then SET, then WHERE, then IF.
            let ts = u
                .using_timestamp
                .as_ref()
                .filter(|t| is_bind(t))
                .map_or(0, |_| 1);
            let ttl = u.using_ttl.as_ref().filter(|t| is_bind(t)).map_or(0, |_| 1);
            let set_count = u
                .assignments
                .iter()
                .filter(|a| match a {
                    Assignment::Simple { value, .. }
                    | Assignment::Add { value, .. }
                    | Assignment::Sub { value, .. } => is_bind(value),
                    Assignment::Element { key, value, .. } => is_bind(key) || is_bind(value),
                })
                .count();
            let where_count = u
                .where_clauses
                .iter()
                .filter(|wc| is_bind(&wc.value))
                .count();
            let if_count = u.if_conditions.iter().filter(|c| is_bind(&c.value)).count();
            ts + ttl + set_count + where_count + if_count
        }
        Statement::Delete(d) => {
            // DELETE syntactic order: USING TIMESTAMP, then WHERE, then IF.
            let ts = d
                .using_timestamp
                .as_ref()
                .filter(|t| is_bind(t))
                .map_or(0, |_| 1);
            let where_count = d
                .where_clauses
                .iter()
                .filter(|wc| is_bind(&wc.value))
                .count();
            let if_count = d.if_conditions.iter().filter(|c| is_bind(&c.value)).count();
            ts + where_count + if_count
        }
        _ => 0,
    }
}

/// Build result column metadata for SELECT statements.
fn build_result_columns(
    select_columns: &[SelectColumn],
    table_meta: Option<&ferrosa_schema::TableMetadata>,
    table_ks: &str,
    table_name: &str,
    state: &SharedState,
) -> Vec<(String, CqlType)> {
    let resolve = |col_name: &str| -> Option<CqlType> {
        table_meta
            .and_then(|table_meta| table_meta.columns.get(col_name))
            .and_then(|col| {
                bridge::parse_cql_type_in_keyspace(&col.column_type, table_ks, &state.schema).ok()
            })
            .or_else(|| system_schema_column_type(table_ks, table_name, col_name))
    };

    let has_star = select_columns
        .iter()
        .any(|c| matches!(c, SelectColumn::Star));

    if has_star {
        if let Some(table_meta) = table_meta {
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
        return system_schema_column_specs(table_ks, table_name);
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
            // Opaque typed values must preserve their exact wire payload across
            // bind substitution. Re-rendering them as debug strings makes later
            // INSERT/UPDATE execution re-parse the value through the wrong type.
            Term::BlobLiteral(encode_value(v))
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
        CqlType::Map(_, _) | CqlType::List(_) | CqlType::Set(_) | CqlType::Vector(_, _) => {
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
fn substitute_bound_values(
    plan: &PreparedPlan,
    mut cursor: &[u8],
    protocol_version: u8,
) -> Result<Statement, CqlError> {
    // Query/EXECUTE parameter flags. CQL native protocol v5 (§4.1.4) widened
    // this field from a 1-byte `[byte]` to a 4-byte `[int]`. Reading the wrong
    // width misaligns the values section, so bind markers never get
    // substituted (driver sends `?` + values, server still sees the marker and
    // rejects it). Bit 0x01 = Values present.
    let flags = if protocol_version >= 0x05 {
        if cursor.remaining() < 4 {
            return Ok(plan.statement.clone());
        }
        cursor.get_u32()
    } else {
        if cursor.remaining() < 1 {
            return Ok(plan.statement.clone());
        }
        cursor.get_u8() as u32
    };
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

/// Substitute bound values for a single statement inside a BATCH.
///
/// BATCH per-statement values use a *different* framing than QUERY/EXECUTE:
/// there is **no leading flags byte** — the section is just
/// `[short n_values]([int len][bytes value])*` (CQL v4/v5 §4.1.7). The cursor
/// is advanced past the consumed values so the caller can continue parsing the
/// next statement (or the trailing consistency level).
fn substitute_batch_values(plan: &PreparedPlan, cursor: &mut &[u8]) -> Result<Statement, CqlError> {
    if cursor.remaining() < 2 {
        return Ok(plan.statement.clone());
    }
    let n_values = cursor.get_u16() as usize;

    let mut bound_terms: Vec<Term> = Vec::with_capacity(n_values);
    for i in 0..n_values {
        if cursor.remaining() < 4 {
            return Err(CqlError::Protocol("BATCH: truncated value length".into()));
        }
        let val_len = cursor.get_i32();
        if val_len < 0 {
            bound_terms.push(Term::Null);
        } else {
            let val_len = val_len as usize;
            if cursor.remaining() < val_len {
                return Err(CqlError::Protocol("BATCH: truncated value bytes".into()));
            }
            let val_bytes = &cursor[..val_len];
            cursor.advance(val_len);

            let cql_type = if i < plan.bound_columns.len() {
                &plan.bound_columns[i].1
            } else {
                &CqlType::Blob
            };
            bound_terms.push(raw_bytes_to_term(cql_type, val_bytes));
        }
    }

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
            // VALUES first, then USING TIMESTAMP/TTL — must match
            // `count_bind_markers` and `analyze_prepared_columns` order.
            for val in &mut i.values {
                substitute_in_term(val, terms, idx);
            }
            if let Some(t) = i.using_timestamp.as_mut() {
                substitute_in_term(t, terms, idx);
            }
            if let Some(t) = i.using_ttl.as_mut() {
                substitute_in_term(t, terms, idx);
            }
            Statement::Insert(i)
        }
        Statement::Update(u) => {
            let mut u = u.clone();
            // UPDATE syntactic order: USING TIMESTAMP/TTL, then SET, then WHERE, then IF.
            if let Some(t) = u.using_timestamp.as_mut() {
                substitute_in_term(t, terms, idx);
            }
            if let Some(t) = u.using_ttl.as_mut() {
                substitute_in_term(t, terms, idx);
            }
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
            for cond in &mut u.if_conditions {
                substitute_in_term(&mut cond.value, terms, idx);
            }
            Statement::Update(u)
        }
        Statement::Delete(d) => {
            let mut d = d.clone();
            // DELETE syntactic order: USING TIMESTAMP, then WHERE, then IF.
            if let Some(t) = d.using_timestamp.as_mut() {
                substitute_in_term(t, terms, idx);
            }
            for wc in &mut d.where_clauses {
                substitute_in_term(&mut wc.value, terms, idx);
            }
            for cond in &mut d.if_conditions {
                substitute_in_term(&mut cond.value, terms, idx);
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
        Term::BindMarker(_) if *idx < terms.len() => {
            *term = terms[*idx].clone();
            *idx += 1;
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

    fn build_minimal_schema_for_test() -> Arc<ferrosa_schema::Schema> {
        use ferrosa_schema::audit::TestAuditSink;
        use ferrosa_schema::{
            AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
            RateLimitConfig, Schema, SchemaConfig,
        };
        let schema = Schema::new(SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .unwrap();
        Arc::new(schema)
    }

    fn shared_state_for_prepare_tests() -> (SharedState, tempfile::TempDir) {
        use arc_swap::ArcSwap;
        use ferrosa_cluster::{DdlPath, ModeController, WritePath};
        use ferrosa_schema::audit::TestAuditSink;
        use ferrosa_schema::{
            AuthMethod, DeploymentMode, EnvSecretsProvider, NodeConfig, PasswordHasher,
            PasswordPolicy, RateLimitConfig, Schema, SchemaConfig,
        };
        use ferrosa_storage::{
            CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig,
            SyncStrategyConfig,
        };

        let dir = tempfile::TempDir::new().unwrap();
        let engine_config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 4096,
                max_segment_age: std::time::Duration::from_secs(60),
                sync_strategy: SyncStrategyConfig::Batch,
                log_dir: dir.path().join("commitlog"),
                checkpoint_dir: dir.path().join("commitlog"),
                archive: None,
            },
            compaction: CompactionConfig::from_env(dir.path().join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.path().to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
            write_verify: false,
        };
        let engine = Arc::new(StorageEngine::new(engine_config, None).unwrap());
        let schema = Arc::new(
            Schema::new(SchemaConfig {
                hasher: PasswordHasher::Bcrypt { cost: 4 },
                password_policy: PasswordPolicy::permissive(),
                auth_method: AuthMethod::Password,
                rate_limit: RateLimitConfig::default(),
                audit_sink: Box::new(TestAuditSink::new()),
                secrets: Box::new(EnvSecretsProvider),
                mode: DeploymentMode::Development,
            })
            .unwrap(),
        );
        let node_config = Arc::new(NodeConfig {
            cluster_name: "test".into(),
            data_center: "datacenter1".into(),
            rack: "rack1".into(),
            rpc_port: 9042,
            host_id: uuid::Uuid::new_v4(),
            listen_address: "127.0.0.1".parse().unwrap(),
            listen_port: 7000,
            broadcast_address: "127.0.0.1".parse().unwrap(),
            broadcast_port: 7000,
            rpc_address: "127.0.0.1".parse().unwrap(),
            internal_rpc_address: "127.0.0.1".parse().unwrap(),
            internal_rpc_port: 9042,
            tokens: vec![],
        });
        let mode_controller = ModeController::standalone_for_test(schema.clone(), engine.clone());
        let udf_executor =
            Arc::new(ferrosa_udf::UdfExecutor::new(ferrosa_udf::SandboxConfig::default()).unwrap());

        (
            SharedState {
                engine: engine.clone(),
                schema: schema.clone(),
                node_config,
                cluster_state: Arc::new(ArcSwap::from_pointee(
                    ferrosa_cluster::ClusterStateHolder::Standalone,
                )),
                write_path: Arc::new(ArcSwap::from_pointee(WritePath::direct(engine.clone()))),
                ddl_path: Arc::new(ArcSwap::from_pointee(DdlPath::Direct { schema, engine })),
                prepared_cache: Arc::new(PreparedCache::new(1024 * 1024)),
                connection_tracker: Arc::new(ConnectionTracker::new()),
                query_tracker: Arc::new(crate::virtual_tables::QueryTracker::new()),
                udf_executor,
                event_sender: tokio::sync::broadcast::channel(64).0,
                mode_controller,
                cql_metrics: Arc::new(crate::observability::CqlMetrics::new()),
                topology_policy: crate::topology::ClientTopologyPolicy::default(),
                auth_warn: false,
                peer_manager: None,
                accord_clock: None,
            },
            dir,
        )
    }

    #[test]
    fn prepare_metadata_resolves_system_schema_keyspace_bind_marker() {
        let (state, _dir) = shared_state_for_prepare_tests();
        let stmt = parser::parse(
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?",
        )
        .unwrap();

        let (bound_columns, result_columns) =
            analyze_prepared_columns(&stmt, "system_schema", "keyspaces", &state);

        assert_eq!(
            bound_columns,
            vec![("keyspace_name".to_string(), CqlType::Varchar)]
        );
        assert_eq!(
            result_columns,
            vec![("keyspace_name".to_string(), CqlType::Varchar)]
        );
    }

    #[tokio::test]
    async fn authenticate_off_runtime_does_not_serialize_on_async_thread() {
        // Pre-fix: `authenticate` ran synchronously on the async worker. On a
        // single-threaded runtime (which `#[tokio::test]` uses), concurrent
        // cost-4 bcrypts serialised through the one worker thread. The old
        // regression assertion used elapsed wall-clock time, which became
        // brittle when the full workspace test runner competed for CPU.
        //
        // The invariant we actually need is deterministic: bcrypt must run on
        // Tokio's blocking pool, not on the async worker thread.
        let schema = build_minimal_schema_for_test();
        let async_worker_thread = std::thread::current().id();
        let (thread_tx, thread_rx) = std::sync::mpsc::channel();

        let mut handles = Vec::new();
        for _ in 0..10 {
            let s = schema.clone();
            let tx = thread_tx.clone();
            handles.push(tokio::spawn(async move {
                authenticate_off_runtime_observed(
                    s,
                    "cassandra".into(),
                    "cassandra".into(),
                    move || tx.send(std::thread::current().id()).unwrap(),
                )
                .await
            }));
        }
        drop(thread_tx);

        for h in handles {
            let res = h.await.unwrap();
            assert!(
                res.is_ok(),
                "auth should succeed against the default cassandra user: {res:?}"
            );
        }

        let blocking_threads: Vec<_> = thread_rx.into_iter().collect();
        assert_eq!(
            blocking_threads.len(),
            10,
            "every auth attempt should enter the blocking closure"
        );
        assert!(
            blocking_threads
                .into_iter()
                .all(|thread_id| thread_id != async_worker_thread),
            "bcrypt auth ran on the async worker thread instead of Tokio's blocking pool"
        );
    }

    #[tokio::test]
    async fn authenticate_off_runtime_returns_failure_for_bad_password() {
        let schema = build_minimal_schema_for_test();
        let result =
            authenticate_off_runtime(schema, "cassandra".into(), "wrong-password".into()).await;
        assert!(matches!(
            result,
            Err(ferrosa_schema::SchemaError::AuthenticationFailed)
        ));
    }

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
        let result = substitute_bound_values(&plan, &payload, 4).unwrap();

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
    fn substitute_batch_values_replaces_bind_markers() {
        // BATCH per-statement values have NO flags byte — just
        // [short n_values]([int len][bytes])*. Regression: handle_batch used to
        // skip these bytes, leaving BindMarkers that route() then rejected.
        let plan = make_plan(
            "INSERT INTO ks.t (id, name) VALUES (?, ?)",
            vec![("id", CqlType::Int), ("name", CqlType::Varchar)],
        );

        let mut payload = Vec::new();
        payload.extend_from_slice(&2u16.to_be_bytes()); // n_values
        payload.extend_from_slice(&4i32.to_be_bytes()); // id len
        payload.extend_from_slice(&7i32.to_be_bytes()); // id = 7
        let name = b"alice";
        payload.extend_from_slice(&(name.len() as i32).to_be_bytes());
        payload.extend_from_slice(name);

        let mut cursor = &payload[..];
        let result = substitute_batch_values(&plan, &mut cursor).unwrap();

        if let Statement::Insert(i) = &result {
            assert!(
                matches!(&i.values[0], Term::IntegerLiteral(7)),
                "first value should be 7, got {:?}",
                i.values[0]
            );
            assert!(
                matches!(&i.values[1], Term::StringLiteral(s) if s == "alice"),
                "second value should be 'alice', got {:?}",
                i.values[1]
            );
            assert!(cursor.is_empty(), "cursor should be fully consumed");
        } else {
            panic!("expected Insert");
        }
    }

    #[test]
    fn substitute_batch_values_handles_null() {
        // A negative length is a NULL bound value → Term::Null (which
        // build_row now writes as a tombstone).
        let plan = make_plan(
            "INSERT INTO ks.t (id, name) VALUES (?, ?)",
            vec![("id", CqlType::Int), ("name", CqlType::Varchar)],
        );
        let mut payload = Vec::new();
        payload.extend_from_slice(&2u16.to_be_bytes());
        payload.extend_from_slice(&4i32.to_be_bytes());
        payload.extend_from_slice(&7i32.to_be_bytes());
        payload.extend_from_slice(&(-1i32).to_be_bytes()); // NULL

        let mut cursor = &payload[..];
        let result = substitute_batch_values(&plan, &mut cursor).unwrap();
        if let Statement::Insert(i) = &result {
            assert!(matches!(&i.values[1], Term::Null), "got {:?}", i.values[1]);
        } else {
            panic!("expected Insert");
        }
    }

    #[test]
    fn substitute_v5_four_byte_flags_replaces_bind_markers() {
        // CQL v5 (§4.1.4) widened the query-parameters <flags> field from a
        // 1-byte [byte] to a 4-byte [int]. gocql and the DataStax Java driver
        // send unprepared queries this way; reading 1 byte misaligns the values
        // section so the bind marker survives and route() rejects it.
        let plan = make_plan(
            "INSERT INTO ks.t (id, name) VALUES (?, ?)",
            vec![("id", CqlType::Int), ("name", CqlType::Varchar)],
        );

        // v5 params: [int flags=0x01][short n_values=2][int len][bytes]...
        let mut payload = Vec::new();
        payload.extend_from_slice(&1u32.to_be_bytes()); // 4-byte flags: VALUES
        payload.extend_from_slice(&2u16.to_be_bytes()); // n_values
        payload.extend_from_slice(&4i32.to_be_bytes());
        payload.extend_from_slice(&7i32.to_be_bytes()); // id = 7
        let name = b"alice";
        payload.extend_from_slice(&(name.len() as i32).to_be_bytes());
        payload.extend_from_slice(name);

        let result = substitute_bound_values(&plan, &payload, 5).unwrap();
        if let Statement::Insert(i) = &result {
            assert!(
                matches!(&i.values[0], Term::IntegerLiteral(7)),
                "got {:?}",
                i.values[0]
            );
            assert!(
                matches!(&i.values[1], Term::StringLiteral(s) if s == "alice"),
                "got {:?}",
                i.values[1]
            );
        } else {
            panic!("expected Insert");
        }

        // The SAME payload read as v4 (1-byte flags) must NOT substitute — proof
        // the width matters and we are not accidentally version-agnostic.
        let v4 = substitute_bound_values(&plan, &payload, 4).unwrap();
        if let Statement::Insert(i) = &v4 {
            assert!(
                matches!(&i.values[0], Term::BindMarker(_)),
                "v4 misread should leave the marker, got {:?}",
                i.values[0]
            );
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
        let result = substitute_bound_values(&plan, &payload, 4).unwrap();

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

        let result = substitute_bound_values(&plan, &payload, 4).unwrap();
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

        let result = substitute_bound_values(&plan, &payload, 4).unwrap();
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

        let result = substitute_bound_values(&plan, &[], 4).unwrap();
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

        let result = substitute_bound_values(&plan, &payload, 4).unwrap();
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

        let result = substitute_bound_values(&plan, &payload, 4).unwrap();

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

        let result = substitute_bound_values(&plan, &payload, 4).unwrap();

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

        let result = substitute_bound_values(&plan, &payload, 4).unwrap();

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

    /// Existing-entity regression: a prepared UPDATE that binds a vector value
    /// must preserve the raw vector payload so the router can decode it back
    /// into the storage cell format. Converting it to a debug string makes the
    /// later UPDATE path reject or mis-handle the embedding.
    #[test]
    fn bind_values_update_preserves_vector_payload() {
        let plan = make_plan(
            "UPDATE ks.entity_store SET entity_embedding = ? WHERE tenant_id = ? AND session_id = ? AND entity_id = ?",
            vec![
                ("entity_embedding", CqlType::Vector(Box::new(CqlType::Float), 3)),
                ("tenant_id", CqlType::Uuid),
                ("session_id", CqlType::Uuid),
                ("entity_id", CqlType::Uuid),
            ],
        );

        let embedding = ferrosa_index::vec_f32_to_bytes(&[1.0, 2.0, 3.0]);
        let tenant_id = uuid::Uuid::from_bytes([0x11; 16]);
        let session_id = uuid::Uuid::from_bytes([0x22; 16]);
        let entity_id = uuid::Uuid::from_bytes([0x33; 16]);
        let payload = encode_values(&[
            &embedding,
            tenant_id.as_bytes(),
            session_id.as_bytes(),
            entity_id.as_bytes(),
        ]);

        let result = substitute_bound_values(&plan, &payload, 4).unwrap();

        if let Statement::Update(u) = &result {
            assert_eq!(u.assignments.len(), 1, "expected one assignment");
            if let Assignment::Simple { value, .. } = &u.assignments[0] {
                assert!(
                    matches!(value, Term::BlobLiteral(bytes) if *bytes == embedding),
                    "vector bind value should remain BlobLiteral(raw-bytes), got {:?}",
                    value
                );
            } else {
                panic!("expected Simple assignment");
            }
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

        let result = substitute_bound_values(&plan, &payload, 4).unwrap();

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

        let result = substitute_bound_values(&plan, &payload, 4).unwrap();

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

    // ── P0-22: count_bind_markers unit tests ─────────────────────────────────
    //
    // Pure AST tests — no schema, no network.  Verify that count_bind_markers
    // returns the exact number of `?` placeholders in each statement type.

    #[test]
    fn count_bind_markers_insert_three_placeholders() {
        let stmt = crate::parser::parse("INSERT INTO ks.t (a, b, c) VALUES (?, ?, ?)").unwrap();
        assert_eq!(
            count_bind_markers(&stmt),
            3,
            "INSERT with 3 ? placeholders must count as 3"
        );
    }

    #[test]
    fn count_bind_markers_insert_eight_placeholders() {
        // Mirrors the fmem entity_store INSERT that triggered p0-22.
        let stmt = crate::parser::parse(
            "INSERT INTO agent_memory.entity_store \
             (tenant_id, session_id, entity_id, entity_name, entity_type, \
              context_snippet, confidence, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .unwrap();
        assert_eq!(
            count_bind_markers(&stmt),
            8,
            "INSERT with 8 ? placeholders must count as 8"
        );
    }

    /// Gap 10: `INSERT ... USING TIMESTAMP ?` must register the timestamp
    /// placeholder so the driver and server agree on bind count.  Without
    /// this fix, ferrosa silently dropped the `USING TIMESTAMP ?` marker
    /// and the DataStax driver got `Too many variables (expected N, got
    /// N+1)` at execute time — see
    /// `ferrosa-nosqlbench/docs/initial-gaps-found.md`.
    #[test]
    fn count_bind_markers_insert_with_using_timestamp_bind() {
        let stmt = crate::parser::parse(
            "INSERT INTO ks.iot (machine_id, sensor_name, time, sensor_value, station_id, data) \
             VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
        )
        .unwrap();
        assert_eq!(
            count_bind_markers(&stmt),
            7,
            "6 value markers + 1 USING TIMESTAMP marker = 7"
        );
    }

    #[test]
    fn count_bind_markers_insert_with_using_timestamp_and_ttl_bind() {
        let stmt = crate::parser::parse(
            "INSERT INTO ks.t (a, b) VALUES (?, ?) USING TIMESTAMP ? AND TTL ?",
        )
        .unwrap();
        assert_eq!(
            count_bind_markers(&stmt),
            4,
            "2 value markers + USING TIMESTAMP ? + USING TTL ? = 4"
        );
    }

    #[test]
    fn count_bind_markers_insert_with_literal_using_does_not_add_placeholder() {
        let stmt = crate::parser::parse(
            "INSERT INTO ks.t (a, b) VALUES (?, ?) USING TIMESTAMP 12345 AND TTL 60",
        )
        .unwrap();
        assert_eq!(
            count_bind_markers(&stmt),
            2,
            "literal USING clauses must NOT count as placeholders"
        );
    }

    #[test]
    fn count_bind_markers_select_one_where_placeholder() {
        let stmt = crate::parser::parse("SELECT * FROM ks.t WHERE a = ?").unwrap();
        assert_eq!(
            count_bind_markers(&stmt),
            1,
            "SELECT with 1 WHERE ? must count as 1"
        );
    }

    #[test]
    fn count_bind_markers_select_ann_of_placeholder() {
        let stmt = crate::parser::parse(
            "SELECT fold_id FROM agent_memory.trajectory_folds \
             WHERE session_id = ? AND tenant_id = ? \
             ORDER BY fold_embedding ANN OF ? LIMIT 5",
        )
        .unwrap();
        assert_eq!(
            count_bind_markers(&stmt),
            3,
            "SELECT with 2 WHERE ? plus ANN OF ? must count as 3"
        );
    }

    #[test]
    fn select_bind_marker_columns_includes_ann_of_bind_marker() {
        let stmt = crate::parser::parse(
            "SELECT fold_id FROM agent_memory.trajectory_folds \
             WHERE session_id = ? AND tenant_id = ? \
             ORDER BY fold_embedding ANN OF ? LIMIT 5",
        )
        .unwrap();

        let Statement::Select(select) = &stmt else {
            panic!("expected SELECT statement");
        };

        assert_eq!(
            select_bind_marker_columns(select),
            vec!["session_id", "tenant_id", "fold_embedding"],
            "PREPARE metadata must include the ANN OF bind marker column after WHERE bind markers"
        );
    }

    #[test]
    fn count_bind_markers_select_no_placeholders() {
        let stmt = crate::parser::parse("SELECT * FROM ks.t").unwrap();
        assert_eq!(
            count_bind_markers(&stmt),
            0,
            "SELECT with no ? must count as 0"
        );
    }

    #[test]
    fn count_bind_markers_update_set_and_where() {
        // 2 SET + 3 WHERE = 5 total
        let stmt =
            crate::parser::parse("UPDATE ks.t SET a = ?, b = ? WHERE c = ? AND d = ? AND e = ?")
                .unwrap();
        assert_eq!(
            count_bind_markers(&stmt),
            5,
            "UPDATE with 2 SET + 3 WHERE ? must count as 5"
        );
    }

    #[test]
    fn count_bind_markers_update_with_if_condition() {
        // 2 SET + 2 WHERE + 1 IF = 5 total
        let stmt = crate::parser::parse(
            "UPDATE ks.meta SET data = ?, version = ? \
             WHERE p = ? AND name = ? IF version = ?",
        )
        .unwrap();
        assert_eq!(
            count_bind_markers(&stmt),
            5,
            "UPDATE...IF with 5 total ? must count as 5"
        );
    }

    #[test]
    fn count_bind_markers_delete_where() {
        let stmt = crate::parser::parse("DELETE FROM ks.t WHERE a = ? AND b = ?").unwrap();
        assert_eq!(
            count_bind_markers(&stmt),
            2,
            "DELETE with 2 WHERE ? must count as 2"
        );
    }
}
