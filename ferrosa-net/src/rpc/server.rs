use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures::stream::SplitSink;
use futures::{FutureExt, SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::codec::{
    Frame, FrameHeader, InternodeCodec, Lane, MsgType, FLAG_FIRE_AND_FORGET, FLAG_RPC_ERROR,
};
use crate::config::NetConfig;
use crate::error::NetError;
use crate::handshake::{accept_handshake, CAP_RPC_ERROR_REPLY};
use crate::message::Message;
use crate::metrics;
use crate::rpc::error_reply::{RemoteFailureKind, RpcErrorReply};
use crate::rpc::handler::{HandlerRegistry, PeerId};
use crate::task_pool::TaskPool;

/// Callback invoked when the server accepts an inbound peer connection.
pub trait InboundPeerCallback: Send + Sync {
    fn on_inbound_peer(
        &self,
        peer_id: PeerId,
        cql_broadcast: Option<String>,
        internode_broadcast: Option<String>,
    );
}

pub struct RpcServer {
    config: Arc<NetConfig>,
    local_host_id: uuid::Uuid,
    registry: Arc<HandlerRegistry>,
    active_connections: Arc<AtomicUsize>,
    cancel: CancellationToken,
    bound_addr: tokio::sync::watch::Sender<Option<std::net::SocketAddr>>,
    #[allow(dead_code)]
    bound_addr_rx: tokio::sync::watch::Receiver<Option<std::net::SocketAddr>>,
    inbound_callback: Option<Arc<dyn InboundPeerCallback>>,
    /// Aggregate bandwidth counters across all inbound connections.
    pub bandwidth: Arc<super::client::BandwidthMetrics>,
    /// Dedicated runtime for non-Raft connections (data, bulk, bootstrap).
    data_runtime: Option<Arc<tokio::runtime::Runtime>>,
    /// Dedicated runtime for Raft RPC handlers.
    raft_runtime: Option<Arc<tokio::runtime::Runtime>>,
}

impl RpcServer {
    pub fn new(
        config: NetConfig,
        local_host_id: uuid::Uuid,
        registry: Arc<HandlerRegistry>,
    ) -> Self {
        let (bound_addr, bound_addr_rx) = tokio::sync::watch::channel(None);
        Self {
            config: Arc::new(config),
            local_host_id,
            registry,
            active_connections: Arc::new(AtomicUsize::new(0)),
            cancel: CancellationToken::new(),
            bound_addr,
            bound_addr_rx,
            inbound_callback: None,
            bandwidth: Arc::new(super::client::BandwidthMetrics::new()),
            data_runtime: None,
            raft_runtime: None,
        }
    }

    /// Set a dedicated runtime for non-Raft connections (data, bulk, bootstrap).
    pub fn with_data_runtime(mut self, rt: Arc<tokio::runtime::Runtime>) -> Self {
        self.data_runtime = Some(rt);
        self
    }

    /// Set a dedicated runtime for Raft RPC handlers.
    pub fn with_raft_runtime(mut self, rt: Arc<tokio::runtime::Runtime>) -> Self {
        self.raft_runtime = Some(rt);
        self
    }

    /// Set a callback for inbound peer connections. Called after handshake succeeds.
    pub fn with_inbound_callback(mut self, cb: Arc<dyn InboundPeerCallback>) -> Self {
        self.inbound_callback = Some(cb);
        self
    }

    fn data_task_pool(&self) -> TaskPool {
        TaskPool::from_optional_runtime("internode-data", self.data_runtime.clone())
    }

    fn raft_task_pool(&self) -> TaskPool {
        TaskPool::from_optional_runtime("internode-raft", self.raft_runtime.clone())
    }

    /// Signal the server to stop accepting new connections and wait for in-flight
    /// connections to drain, up to `drain_timeout`. Any connections still active after
    /// the timeout are abandoned (the OS will close the socket).
    ///
    /// # Cancel Safety
    ///
    /// This method is cancel-safe. Shutdown is signalled via `CancellationToken::cancel`,
    /// which is an instantaneous, idempotent operation. In-flight connections run to
    /// completion within the drain window regardless of whether this future is dropped.
    pub async fn shutdown(&self, drain_timeout: Duration) {
        self.cancel.cancel();
        tokio::time::timeout(drain_timeout, self.wait_for_connections())
            .await
            .ok();
    }

    /// Busy-poll until no active connections remain.
    async fn wait_for_connections(&self) {
        loop {
            if self.active_connections.load(Ordering::Acquire) == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Build TLS acceptor and store it, then start listening.
    pub async fn start_and_get_addr(
        self: &Arc<Self>,
    ) -> crate::error::Result<std::net::SocketAddr> {
        // Build TLS acceptor if configured (must happen before spawning)
        let tls_acceptor = crate::tls::build_tls_acceptor(&self.config)?;

        // Create the TCP listener on the Data runtime (if available) so ALL
        // connection handlers and their frame readers run there — not on the
        // main runtime where they'd compete with CQL/bootstrap work.
        let bind_addr = self.config.bind_addr;
        let server = self.clone();
        let data_task_pool = self.data_task_pool();

        if self.data_runtime.is_some() {
            let bound_addr_tx = self.bound_addr.clone();
            data_task_pool.spawn(async move {
                match TcpListener::bind(bind_addr).await {
                    Ok(listener) => {
                        let addr = match listener.local_addr() {
                            Ok(addr) => addr,
                            Err(e) => {
                                tracing::error!(%e, "failed to read local_addr after bind");
                                // Best-effort signal — the parent's rx.changed()
                                // will return Err and surface as StartupFailed.
                                if bound_addr_tx.send(None).is_err() {
                                    tracing::error!(
                                        "bound_addr receiver dropped before failure could be reported"
                                    );
                                }
                                return;
                            }
                        };
                        if let Err(e) = bound_addr_tx.send(Some(addr)) {
                            // Receiver gone — parent gave up before bind completed.
                            // Continue serving anyway (some other peer may still
                            // connect), but loudly record the lost notification
                            // so a confused parent caller is observable.
                            tracing::error!(
                                %addr,
                                error = %e,
                                "bound_addr receiver dropped; caller will see StartupFailed"
                            );
                        }
                        tracing::info!(%addr, "internode server listening (data runtime)");
                        server.accept_loop(listener, tls_acceptor).await;
                    }
                    Err(e) => {
                        // Surface BUG-001-style guidance: a port-7000 EADDRINUSE
                        // on macOS is almost certainly ControlCenter, not user
                        // contention. The diagnostic prints the actionable fix.
                        let diag = crate::error::bind_failure_diagnostic(&bind_addr, &e);
                        tracing::error!(diag = %diag, %e, "failed to bind internode server");
                        // Notify the waiting caller so it doesn't hang forever.
                        if bound_addr_tx.send(None).is_err() {
                            tracing::error!(
                                "bound_addr receiver dropped before bind error could be reported"
                            );
                        }
                    }
                }
            });
            // Wait for the bind to complete (either Some(addr) or None on failure).
            let mut rx = self.bound_addr_rx.clone();
            rx.changed().await.map_err(|_| {
                NetError::StartupFailed("bound_addr channel closed before bind completed".into())
            })?;
            let bound = *rx.borrow_and_update();
            match bound {
                Some(addr) => Ok(addr),
                None => Err(NetError::StartupFailed(
                    "internode server failed to bind (see logs)".into(),
                )),
            }
        } else {
            let listener = match TcpListener::bind(bind_addr).await {
                Ok(l) => l,
                Err(e) => {
                    let diag = crate::error::bind_failure_diagnostic(&bind_addr, &e);
                    tracing::error!(diag = %diag, %e, "failed to bind internode server");
                    return Err(NetError::Io(e));
                }
            };
            let addr = listener.local_addr()?;
            // No external waiter on this branch — bound_addr is updated for any
            // future subscribers and to keep the watch state coherent. If the
            // send fails it means no subscribers existed yet, which is benign
            // here (Ok(addr) is returned synchronously below). Log so the
            // event is still observable.
            if let Err(e) = self.bound_addr.send(Some(addr)) {
                tracing::debug!(
                    %addr,
                    error = %e,
                    "bound_addr update had no subscribers (synchronous start path)"
                );
            }
            tracing::info!(%addr, "internode server listening");
            data_task_pool.spawn(async move { server.accept_loop(listener, tls_acceptor).await });
            Ok(addr)
        }
    }

    async fn accept_loop(
        self: Arc<Self>,
        listener: TcpListener,
        tls_acceptor: Option<TlsAcceptor>,
    ) {
        loop {
            let (stream, peer_addr) = tokio::select! {
                _ = self.cancel.cancelled() => {
                    tracing::info!("RpcServer: stopping accept loop");
                    break;
                }
                result = listener.accept() => {
                    match result {
                        Ok(conn) => conn,
                        Err(e) => {
                            tracing::error!(error = %e, "accept error");
                            continue;
                        }
                    }
                }
            };

            let current = self.active_connections.load(Ordering::Relaxed);
            if current >= self.config.max_connections {
                tracing::warn!(%peer_addr, "rejecting: max connections reached");
                let config = self.config.clone();
                let host_id = self.local_host_id;
                self.data_task_pool().spawn(async move {
                    let mut framed =
                        Framed::new(stream, InternodeCodec::new(config.max_frame_body_size));
                    if let Some(Ok(_frame)) = framed.next().await {
                        let ack = Message::HandshakeAck {
                            host_id,
                            protocol_version: crate::handshake::PROTOCOL_VERSION,
                            chosen_compression: 0,
                            accepted: false,
                            reason: "overloaded".to_string(),
                            cql_broadcast: None,
                            internode_broadcast: None,
                            capabilities: config.advertised_capabilities,
                        };
                        let mut body = bytes::BytesMut::new();
                        if let Err(e) = ack.encode(&mut body) {
                            tracing::error!(%peer_addr, %e, "overload rejection ack failed to encode");
                            return;
                        }
                        let body_len = u32::try_from(body.len())
                            .expect("a HandshakeAck body is a few hundred bytes");
                        let frame = Frame {
                            header: FrameHeader::new(
                                MsgType::HandshakeAck,
                                Lane::Raft,
                                0,
                                body_len,
                            ),
                            body: body.freeze(),
                        };
                        if let Err(e) = framed.send(frame).await {
                            tracing::warn!(%peer_addr, %e, "overload rejection ack was not delivered");
                        }
                    }
                });
                continue;
            }

            self.active_connections.fetch_add(1, Ordering::Relaxed);
            let server = self.clone();
            let tls_acceptor = tls_acceptor.clone();
            self.data_task_pool().spawn(async move {
                let result = if let Some(acceptor) = tls_acceptor {
                    match tokio::time::timeout(Duration::from_secs(10), acceptor.accept(stream))
                        .await
                    {
                        Ok(Ok(tls_stream)) => server.handle_connection(tls_stream, peer_addr).await,
                        Ok(Err(e)) => {
                            tracing::warn!(%peer_addr, "TLS handshake failed: {e}");
                            Err(NetError::Protocol(format!("TLS handshake failed: {e}")))
                        }
                        Err(_) => {
                            tracing::warn!(%peer_addr, "TLS handshake timeout");
                            Err(NetError::Timeout("TLS handshake".into()))
                        }
                    }
                } else {
                    server.handle_connection(stream, peer_addr).await
                };
                if let Err(e) = result {
                    tracing::error!(%peer_addr, error = %e, "connection error");
                }
                server.active_connections.fetch_sub(1, Ordering::Relaxed);
            });
        }
    }

    async fn handle_connection<S>(
        &self,
        stream: S,
        peer_addr: std::net::SocketAddr,
    ) -> crate::error::Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let mut framed = Framed::new(stream, InternodeCodec::new(self.config.max_frame_body_size));

        // Handshake with timeout (T5)
        let peer = tokio::time::timeout(
            self.config.handshake_timeout,
            accept_handshake(&mut framed, &self.config, self.local_host_id),
        )
        .await
        .map_err(|_| NetError::Timeout("handshake".into()))??;

        let peer_id = (peer.host_id, peer_addr);
        tracing::info!(?peer_id, "peer connected (inbound)");

        // Notify callback about inbound peer
        if let Some(cb) = &self.inbound_callback {
            cb.on_inbound_peer(peer_id, peer.cql_broadcast, peer.internode_broadcast);
        }

        let error_replies = peer.capabilities & CAP_RPC_ERROR_REPLY != 0;

        // Concurrent frame handling: split read/write, dispatch handlers
        // to the Data runtime so they don't block frame reading.
        let (sink, mut stream) = framed.split();
        let registry = self.registry.clone();
        let bandwidth = self.bandwidth.clone();

        // Frame reading stays on the main runtime (tokio I/O handles can't
        // move between runtimes). Handlers are dispatched to dedicated runtimes.
        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel::<Frame>(64);
        let write_task =
            self.data_task_pool()
                .spawn(write_responses(sink, resp_rx, bandwidth.clone(), peer_id));

        while let Some(frame_result) = stream.next().await {
            let frame = match frame_result {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!(?peer_id, %e, "frame read error");
                    break;
                }
            };
            bandwidth.bytes_received.fetch_add(
                frame.body.len() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            let msg_type = frame.header.msg_type;
            let route = ReplyRoute {
                peer_id,
                msg_type,
                lane: frame.header.lane,
                stream_id: frame.header.stream_id,
                fire_and_forget: frame.header.flags & FLAG_FIRE_AND_FORGET != 0,
                error_replies,
                max_frame_body_size: self.config.max_frame_body_size,
                resp_tx: resp_tx.clone(),
            };
            let msg = match Message::decode(msg_type, &mut frame.body.clone()) {
                Ok(m) => m,
                Err(e) => {
                    route
                        .fail(RemoteFailureKind::RequestDecodeFailed, &e.to_string())
                        .await;
                    continue;
                }
            };

            // Dispatch handlers to the appropriate runtime.
            // Raft handlers → Raft runtime (isolated from data-path load).
            // Data/Bulk handlers → Data runtime (isolated from main runtime).
            // Fallback → main runtime.
            let handler = run_handler(registry.clone(), route, msg);

            if msg_type.is_ordered_stream_response() {
                // Frames of one streaming range-read response (chunk /
                // heartbeat / done) form a single ordered, per-request
                // stream. The coordinator's StreamFrameRouter enforces a
                // strict, contiguous chunk `seq` and closes the route on
                // the first gap — so spawning one task per frame here lets
                // chunk seq=N+1 overtake seq=N under tokio scheduling and
                // trips that check mid-stream (ChannelClosedBeforeDone),
                // which a wide-partition scan reliably hits once the
                // response spans many row-capped chunks. Run the handler
                // inline to preserve wire order. It is non-blocking
                // (decode + StreamRouter::route via try_send), so it
                // cannot stall frame reading the way the producer-side
                // RangeReadStreamRequest storage read would. `run_handler`
                // catches a panic, so one cannot unwind through this loop and
                // take the whole connection down.
                handler.await;
            } else if msg_type.is_raft() {
                self.raft_task_pool().spawn(handler);
            } else {
                self.data_task_pool().spawn(handler);
            }
        }

        drop(resp_tx);
        if let Err(e) = write_task.await {
            tracing::error!(?peer_id, %e, "internode response writer task failed");
        }

        tracing::info!(?peer_id, "peer disconnected");
        Ok(())
    }
}

/// Drain one connection's response frames onto its socket.
async fn write_responses<S>(
    mut sink: SplitSink<Framed<S, InternodeCodec>, Frame>,
    mut resp_rx: mpsc::Receiver<Frame>,
    bandwidth: Arc<super::client::BandwidthMetrics>,
    peer_id: PeerId,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    while let Some(frame) = resp_rx.recv().await {
        bandwidth
            .bytes_sent
            .fetch_add(frame.body.len() as u64, Ordering::Relaxed);
        if let Err(e) = sink.send(frame).await {
            // Handlers still running for this connection see their send fail
            // and log it; the requester sees the connection close.
            tracing::warn!(?peer_id, %e, "internode response write failed; closing the writer");
            break;
        }
    }
}

/// Where and how to answer one inbound request frame.
struct ReplyRoute {
    peer_id: PeerId,
    msg_type: MsgType,
    lane: Lane,
    stream_id: u32,
    /// The requester expects no answer, so a failure can only be logged.
    fire_and_forget: bool,
    /// The requester advertised [`CAP_RPC_ERROR_REPLY`].
    error_replies: bool,
    max_frame_body_size: u32,
    resp_tx: mpsc::Sender<Frame>,
}

impl ReplyRoute {
    /// Send the handler's response, or an error reply if it cannot be framed.
    async fn respond(&self, response: Message) {
        let response_type = response.msg_type();
        let mut body = BytesMut::new();
        if let Err(e) = response.encode(&mut body) {
            let detail = format!("{response_type:?} response failed to encode: {e}");
            self.fail(RemoteFailureKind::ResponseEncodeFailed, &detail)
                .await;
            return;
        }
        let framable = u32::try_from(body.len())
            .ok()
            .filter(|len| *len <= self.max_frame_body_size);
        let Some(body_len) = framable else {
            let detail = format!(
                "{response_type:?} response is {} bytes; the frame limit is {}",
                body.len(),
                self.max_frame_body_size
            );
            self.fail(RemoteFailureKind::ResponseTooLarge, &detail)
                .await;
            return;
        };
        let header = FrameHeader::new(response_type, self.lane, self.stream_id, body_len);
        self.send(Frame {
            header,
            body: body.freeze(),
        })
        .await;
    }

    /// Log and count a request that will get no response, then tell the
    /// requester so it releases its stream slot now rather than at its lane
    /// timeout. Fire-and-forget requests and peers that predate error replies
    /// get the log line only.
    async fn fail(&self, kind: RemoteFailureKind, detail: &str) {
        metrics::record_rpc_handler_failure(kind);
        let reply_sent = !self.fire_and_forget && self.error_replies;
        let requester_outcome = if self.fire_and_forget {
            "none: fire-and-forget"
        } else if reply_sent {
            "error reply sent"
        } else {
            "none: peer predates error replies and fails at its lane timeout"
        };
        tracing::error!(
            peer = ?self.peer_id,
            msg_type = ?self.msg_type,
            stream_id = self.stream_id,
            %kind,
            detail,
            requester_outcome,
            "internode RPC request failed"
        );
        if !reply_sent {
            return;
        }
        let reply = RpcErrorReply::new(kind, detail);
        let mut body = BytesMut::new();
        if let Err(e) = reply.encode(&mut body) {
            tracing::error!(
                peer = ?self.peer_id,
                stream_id = self.stream_id,
                %e,
                "error reply failed to encode; the requester fails at its lane timeout"
            );
            return;
        }
        let body_len = u32::try_from(body.len())
            .expect("an error reply body is bounded by MAX_DETAIL_BYTES plus 3 bytes");
        let mut header = FrameHeader::new(self.msg_type, self.lane, self.stream_id, body_len);
        header.flags |= FLAG_RPC_ERROR;
        self.send(Frame {
            header,
            body: body.freeze(),
        })
        .await;
    }

    async fn send(&self, frame: Frame) {
        if self.resp_tx.send(frame).await.is_err() {
            tracing::warn!(
                peer = ?self.peer_id,
                msg_type = ?self.msg_type,
                stream_id = self.stream_id,
                "internode reply dropped: the connection's writer has exited; the \
                 requester fails when it sees the connection close"
            );
        }
    }
}

/// Run one request's handler and answer it, whatever the handler does.
///
/// A panic is caught here, at the dispatch boundary, instead of unwinding
/// into the spawned task (which dropped the reply and left the requester's
/// slot held until its lane timeout) or into the connection reader (which, on
/// the inline ordered-stream path, closed the whole connection). It stays
/// loud: an ERROR line with the panic message, the panic hook's own output,
/// and `ferrosa_net_rpc_handler_panics_total{msg_type}`.
async fn run_handler(registry: Arc<HandlerRegistry>, route: ReplyRoute, msg: Message) {
    // AssertUnwindSafe: after a panic nothing here touches the handler's
    // state again; we only report it. Shared state the handler left
    // half-updated is no less exposed than it was when tokio caught the same
    // panic at the task boundary.
    let dispatched = AssertUnwindSafe(registry.dispatch(route.peer_id, route.msg_type, msg))
        .catch_unwind()
        .await;
    match dispatched {
        Ok(Some(response)) => route.respond(response).await,
        Ok(None) => {}
        Err(payload) => {
            metrics::record_rpc_handler_panic(route.msg_type);
            let detail = panic_message(payload.as_ref());
            route
                .fail(RemoteFailureKind::HandlerPanicked, &detail)
                .await;
        }
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic payload is not a string".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{Lane, MsgType};
    use crate::config::NetConfig;
    use crate::handshake::initiate_handshake;
    use crate::message::Message;
    use crate::rpc::handler::{HandlerRegistry, PeerId, RpcHandler};
    use bytes::BytesMut;
    use std::sync::Arc;
    use tokio::net::TcpStream;
    use tokio_util::codec::Framed;

    struct EchoPingHandler;

    #[async_trait::async_trait]
    impl RpcHandler for EchoPingHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            match msg {
                Message::Ping { nonce, .. } => Some(Message::Pong {
                    nonce,
                    ping_recv_at: 0,
                    sent_at: 0,
                }),
                _ => None,
            }
        }
    }

    #[tokio::test]
    async fn server_accepts_connection_and_completes_handshake() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));

        let addr = server.start_and_get_addr().await.unwrap();
        let client_id = uuid::Uuid::new_v4();
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut framed = Framed::new(stream, InternodeCodec::new(config.max_frame_body_size));
        let peer = initiate_handshake(&mut framed, &config, client_id)
            .await
            .unwrap();
        assert_eq!(peer.host_id, server_id);
    }

    #[tokio::test]
    async fn server_rejects_when_max_connections_reached() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_connections: 1,
            ..NetConfig::default()
        };
        let server_id = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));

        let addr = server.start_and_get_addr().await.unwrap();

        // First connection succeeds
        let stream1 = TcpStream::connect(addr).await.unwrap();
        let mut framed1 = Framed::new(stream1, InternodeCodec::new(config.max_frame_body_size));
        initiate_handshake(&mut framed1, &config, uuid::Uuid::new_v4())
            .await
            .unwrap();

        // Second connection: should be rejected with HandshakeFailed
        let stream2 = TcpStream::connect(addr).await.unwrap();
        let mut framed2 = Framed::new(stream2, InternodeCodec::new(config.max_frame_body_size));
        let result = initiate_handshake(&mut framed2, &config, uuid::Uuid::new_v4()).await;
        assert!(matches!(result, Err(NetError::HandshakeFailed(_))));
    }

    #[tokio::test]
    async fn server_dispatches_message_to_handler() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));

        let addr = server.start_and_get_addr().await.unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut framed = Framed::new(stream, InternodeCodec::new(config.max_frame_body_size));
        initiate_handshake(&mut framed, &config, uuid::Uuid::new_v4())
            .await
            .unwrap();

        // Send Ping
        use futures::{SinkExt, StreamExt};
        let ping = Message::Ping {
            nonce: 42,
            sent_at: 0,
        };
        let mut body = BytesMut::new();
        ping.encode(&mut body).unwrap();
        let frame = Frame {
            header: FrameHeader::new(
                MsgType::Ping,
                Lane::Raft,
                1,
                u32::try_from(body.len()).unwrap(),
            ),
            body: body.freeze(),
        };
        framed.send(frame).await.unwrap();

        // Receive Pong
        let resp_frame = framed.next().await.unwrap().unwrap();
        let resp =
            Message::decode(resp_frame.header.msg_type, &mut resp_frame.body.clone()).unwrap();
        assert!(matches!(resp, Message::Pong { nonce: 42, .. }));
    }

    /// After shutdown(), the listener should stop accepting new connections.
    #[tokio::test]
    async fn shutdown_stops_accepting_connections() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));

        let addr = server.start_and_get_addr().await.unwrap();

        // Confirm the server is up: a connection + handshake should succeed.
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut framed = Framed::new(stream, InternodeCodec::new(config.max_frame_body_size));
        initiate_handshake(&mut framed, &config, uuid::Uuid::new_v4())
            .await
            .unwrap();
        // Drop the framed connection so the server-side handler exits and the
        // active_connections counter returns to zero before we call shutdown.
        drop(framed);

        // Give the connection handler a moment to decrement the counter.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Shut down with a generous drain timeout.
        server.shutdown(Duration::from_millis(200)).await;

        // After shutdown the accept loop has exited, so the listener is dropped.
        // New connection attempts should be refused at the OS level (connection
        // refused) or at least the server will not process them.
        let connect_result =
            tokio::time::timeout(Duration::from_millis(200), TcpStream::connect(addr)).await;

        match connect_result {
            // Connection refused — OS already closed the port.
            Ok(Err(_)) => {}
            // Timeout — the listen socket is gone but the OS hasn't recycled the port yet.
            Err(_) => {}
            // Connected — verify the server no longer performs a handshake by reading EOF.
            Ok(Ok(stream)) => {
                let mut framed2 =
                    Framed::new(stream, InternodeCodec::new(config.max_frame_body_size));
                // The accept loop is not running, so no handshake frame will arrive.
                // framed.next() should return None (EOF) quickly.
                let frame = tokio::time::timeout(Duration::from_millis(200), framed2.next()).await;
                // Either timeout or EOF — either way, no new connection is served.
                assert!(
                    frame.is_err() || frame.unwrap().is_none(),
                    "server must not serve connections after shutdown"
                );
            }
        }
    }

    /// A slow handler that blocks for a configurable duration, used to hold a
    /// connection open while shutdown drains.
    struct SlowHandler {
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl RpcHandler for SlowHandler {
        async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
            tokio::time::sleep(self.delay).await;
            match msg {
                Message::Ping { nonce, .. } => Some(Message::Pong {
                    nonce,
                    ping_recv_at: 0,
                    sent_at: 0,
                }),
                _ => None,
            }
        }
    }

    /// shutdown() should wait for an in-flight handler to complete before returning
    /// when the drain timeout is long enough.
    #[tokio::test]
    async fn shutdown_waits_for_inflight() {
        let handler_delay = Duration::from_millis(80);
        let drain_timeout = Duration::from_millis(500);

        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(
            MsgType::Ping,
            Arc::new(SlowHandler {
                delay: handler_delay,
            }),
        );
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));

        let addr = server.start_and_get_addr().await.unwrap();

        // Connect and complete handshake so an active connection is counted.
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut framed = Framed::new(stream, InternodeCodec::new(config.max_frame_body_size));
        initiate_handshake(&mut framed, &config, uuid::Uuid::new_v4())
            .await
            .unwrap();

        // Send a Ping which will be processed slowly by SlowHandler.
        use futures::SinkExt;
        let ping = Message::Ping {
            nonce: 99,
            sent_at: 0,
        };
        let mut body = BytesMut::new();
        ping.encode(&mut body).unwrap();
        let frame = Frame {
            header: FrameHeader::new(
                MsgType::Ping,
                Lane::Raft,
                1,
                u32::try_from(body.len()).unwrap(),
            ),
            body: body.freeze(),
        };
        framed.send(frame).await.unwrap();

        // Kick off shutdown concurrently. The drain window is long enough that
        // the slow handler should finish inside it.
        let server_clone = server.clone();
        let shutdown_handle = tokio::spawn(async move {
            server_clone.shutdown(drain_timeout).await;
        });

        // Receive the Pong — proves the handler ran to completion.
        let resp_frame = tokio::time::timeout(drain_timeout, framed.next())
            .await
            .expect("expected pong before drain timeout")
            .expect("stream should not be closed")
            .expect("expected valid frame");
        let resp =
            Message::decode(resp_frame.header.msg_type, &mut resp_frame.body.clone()).unwrap();
        assert!(
            matches!(resp, Message::Pong { nonce: 99, .. }),
            "expected Pong nonce=99"
        );

        // Drop our end so the server-side connection task exits and the counter
        // goes to zero, letting shutdown() complete.
        drop(framed);
        shutdown_handle.await.unwrap();

        // After shutdown the active connection counter must be zero.
        assert_eq!(server.active_connections.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn bandwidth_metrics_track_bytes() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));

        let addr = server.start_and_get_addr().await.unwrap();

        let client =
            crate::rpc::client::RpcClient::connect(Arc::new(config), uuid::Uuid::new_v4(), addr)
                .await
                .unwrap();

        let _resp = client
            .send(
                Message::Ping {
                    nonce: 7,
                    sent_at: 0,
                },
                Lane::Raft,
            )
            .await
            .unwrap();

        // Client should have tracked bytes sent > 0.
        let sent = client
            .bandwidth
            .bytes_sent
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(sent > 0, "client bytes_sent should be > 0, got {sent}");

        // Client should have tracked bytes received > 0.
        let received = client
            .bandwidth
            .bytes_received
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            received > 0,
            "client bytes_received should be > 0, got {received}"
        );

        // Server-side bandwidth counters may need a small delay for the
        // dispatch loop to process; check that the server has the counters
        // available (non-zero after processing at least one request).
        tokio::time::sleep(Duration::from_millis(50)).await;
        let server_recv = server
            .bandwidth
            .bytes_received
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            server_recv > 0,
            "server bytes_received should be > 0, got {server_recv}"
        );
    }

    /// P0-07: bind on an unavailable port returns NetError::StartupFailed,
    /// not a hang or a Protocol error. The synchronous start branch
    /// (no data_runtime) surfaces the I/O error directly via `?`.
    #[tokio::test]
    async fn bind_failure_returns_startup_or_io_error_sync_path() {
        // Hold a listener on a real port, then try to bind to the same port.
        let blocker = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let blocked_addr = blocker.local_addr().unwrap();

        let registry = Arc::new(HandlerRegistry::new());
        let config = NetConfig {
            bind_addr: blocked_addr,
            ..NetConfig::default()
        };
        let server = Arc::new(RpcServer::new(config, uuid::Uuid::new_v4(), registry));

        let res = server.start_and_get_addr().await;
        assert!(
            res.is_err(),
            "binding to an in-use port should fail, got: {res:?}"
        );
        // Sync path: I/O error from TcpListener::bind propagates as NetError::Io.
        match res {
            Err(NetError::Io(_)) | Err(NetError::StartupFailed(_)) => {}
            other => panic!("expected Io or StartupFailed, got {other:?}"),
        }
        drop(blocker);
    }

    /// P0-07: end-to-end check that two nodes can connect over the new code
    /// path — minimum multinode (pair) coverage so the bind-notification +
    /// alive-channel changes are exercised against a live peer.
    #[tokio::test]
    async fn pair_nodes_handshake_and_round_trip() {
        struct EchoPing;
        #[async_trait::async_trait]
        impl RpcHandler for EchoPing {
            async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
                match msg {
                    Message::Ping { nonce, .. } => Some(Message::Pong {
                        nonce,
                        ping_recv_at: 0,
                        sent_at: 0,
                    }),
                    _ => None,
                }
            }
        }

        // Node A and Node B both run the new bind path.
        let node_a_id = uuid::Uuid::new_v4();
        let node_b_id = uuid::Uuid::new_v4();

        let mk_server = |node_id| {
            let registry = Arc::new(HandlerRegistry::new());
            registry.register(MsgType::Ping, Arc::new(EchoPing));
            let config = NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                ..NetConfig::default()
            };
            (
                Arc::new(RpcServer::new(config.clone(), node_id, registry)),
                Arc::new(config),
            )
        };

        let (server_a, config_a) = mk_server(node_a_id);
        let (server_b, _config_b) = mk_server(node_b_id);

        let addr_a = server_a.start_and_get_addr().await.unwrap();
        let addr_b = server_b.start_and_get_addr().await.unwrap();
        assert_ne!(addr_a, addr_b);

        // A → B
        let client_ab = crate::rpc::client::RpcClient::connect(config_a.clone(), node_a_id, addr_b)
            .await
            .unwrap();
        let resp = client_ab
            .send(
                Message::Ping {
                    nonce: 1,
                    sent_at: 0,
                },
                crate::codec::Lane::Data,
            )
            .await
            .unwrap();
        assert!(matches!(resp, Message::Pong { nonce: 1, .. }));

        // B → A (proves the bind-notification path on both sides delivered).
        let client_ba = crate::rpc::client::RpcClient::connect(config_a, node_b_id, addr_a)
            .await
            .unwrap();
        let resp = client_ba
            .send(
                Message::Ping {
                    nonce: 2,
                    sent_at: 0,
                },
                crate::codec::Lane::Data,
            )
            .await
            .unwrap();
        assert!(matches!(resp, Message::Pong { nonce: 2, .. }));
    }

    /// Regression (range-stream chunk reorder closes route): frames of a
    /// single streaming range-read response must be dispatched in wire
    /// order. The coordinator's `StreamFrameRouter` enforces a strict,
    /// contiguous chunk `seq` and closes the route on the first gap, so a
    /// reorder here surfaces downstream as `ChannelClosedBeforeDone` — the
    /// fault a wide-partition scan (many row-capped chunks) reliably hits.
    ///
    /// This handler sleeps LONGER for earlier markers. Under the old
    /// one-task-per-frame dispatch the later (shorter-sleep) frames finish
    /// first, recording `[2, 1, 0]`. In-order inline dispatch makes the
    /// reader await each stream-response handler before reading the next
    /// frame, so the recorded order is exactly the wire order `[0, 1, 2]`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stream_response_frames_dispatch_in_wire_order() {
        use futures::SinkExt;

        struct OrderRecorder {
            order: Arc<std::sync::Mutex<Vec<u8>>>,
        }
        #[async_trait::async_trait]
        impl RpcHandler for OrderRecorder {
            async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
                if let Message::RangeReadStreamChunk(body) = msg {
                    let marker = body.first().copied().unwrap_or(0);
                    // Earlier markers sleep longer: any concurrency would
                    // let later frames record ahead of earlier ones.
                    tokio::time::sleep(Duration::from_millis(50 * (3 - marker as u64))).await;
                    self.order.lock().unwrap().push(marker);
                }
                None
            }
        }

        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let server_id = uuid::Uuid::new_v4();
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(
            MsgType::RangeReadStreamChunk,
            Arc::new(OrderRecorder {
                order: order.clone(),
            }),
        );
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));

        let addr = server.start_and_get_addr().await.unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut framed = Framed::new(stream, InternodeCodec::new(config.max_frame_body_size));
        initiate_handshake(&mut framed, &config, uuid::Uuid::new_v4())
            .await
            .unwrap();

        // Three chunk frames, markers 0,1,2, sent back-to-back in order.
        for marker in 0u8..3 {
            let body = bytes::Bytes::copy_from_slice(&[marker]);
            let frame = Frame {
                header: FrameHeader::new(
                    MsgType::RangeReadStreamChunk,
                    Lane::Bulk,
                    1,
                    u32::try_from(body.len()).unwrap(),
                ),
                body,
            };
            framed.send(frame).await.unwrap();
        }

        // Wait until all three frames have been recorded (bounded).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if order.lock().unwrap().len() == 3 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "handler did not observe all three chunk frames within the deadline"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(
            *order.lock().unwrap(),
            vec![0, 1, 2],
            "stream-response frames must be dispatched in wire order; a reorder \
             closes the StreamFrameRouter route mid-stream (ChannelClosedBeforeDone)"
        );
    }

    /// 2026-10-03: inbound handlers panicked and the requester got no frame at
    /// all, so each request held its stream slot until the lane timeout and
    /// node1's lanes sat pinned at their in-flight cap. These tests run over an
    /// in-memory duplex on tokio's paused clock: a request that is never
    /// answered surfaces as a lane timeout after the clock auto-advances, so a
    /// missing reply fails the test instead of hanging it, with no wall-clock
    /// waiting.
    mod failure_replies {
        use super::*;
        use crate::codec::{FLAG_FIRE_AND_FORGET, FLAG_RPC_ERROR};
        use crate::lane_actor::{spawn_lane_actor, ActorReconnectContext};
        use crate::metrics;
        use crate::reconnect::LaneState;
        use crate::rpc::client::RpcClient;
        use crate::rpc::error_reply::{RemoteFailureKind, RpcErrorReply};
        use crate::task_pool::TaskPool;
        use futures::{SinkExt, StreamExt};
        use tokio::io::DuplexStream;

        /// Bound for "no frame arrives": far beyond every lane timeout, and
        /// free on the paused clock.
        const SILENCE: Duration = Duration::from_secs(600);

        fn peer_addr() -> std::net::SocketAddr {
            "127.0.0.1:7999".parse().unwrap()
        }

        fn ping(nonce: u64) -> Message {
            Message::Ping { nonce, sent_at: 0 }
        }

        /// Answers a Ping with a Pong, except nonce 0. Panics on nonce 0 and on
        /// every other message type.
        struct PanicOnZero;

        #[async_trait::async_trait]
        impl RpcHandler for PanicOnZero {
            async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
                match msg {
                    Message::Ping { nonce: 0, .. } => panic!("handler exploded on nonce 0"),
                    Message::Ping { nonce, .. } => Some(Message::Pong {
                        nonce,
                        ping_recv_at: 0,
                        sent_at: 0,
                    }),
                    other => panic!("handler exploded on {:?}", other.msg_type()),
                }
            }
        }

        /// Returns a response whose `reason` exceeds the u16 string limit, so
        /// `Message::encode` fails.
        struct UnencodableResponse;

        #[async_trait::async_trait]
        impl RpcHandler for UnencodableResponse {
            async fn handle(&self, _from: PeerId, _msg: Message) -> Option<Message> {
                Some(Message::HandshakeAck {
                    host_id: uuid::Uuid::nil(),
                    protocol_version: 1,
                    chosen_compression: 0,
                    accepted: true,
                    reason: "x".repeat(70_000),
                    cql_broadcast: None,
                    internode_broadcast: None,
                    capabilities: 0,
                })
            }
        }

        /// Never completes: models a peer that will not answer.
        struct NeverAnswers;

        #[async_trait::async_trait]
        impl RpcHandler for NeverAnswers {
            async fn handle(&self, _from: PeerId, _msg: Message) -> Option<Message> {
                futures::future::pending::<()>().await;
                None
            }
        }

        /// Serve one inbound connection with `registry`; return the client end.
        fn serve(registry: Arc<HandlerRegistry>) -> DuplexStream {
            let server = Arc::new(RpcServer::new(
                NetConfig::default(),
                uuid::Uuid::new_v4(),
                registry,
            ));
            let (client_io, server_io) = tokio::io::duplex(1 << 20);
            tokio::spawn(async move {
                if let Err(e) = server.handle_connection(server_io, peer_addr()).await {
                    tracing::error!(%e, "test server connection ended with an error");
                }
            });
            client_io
        }

        async fn connected_client(registry: Arc<HandlerRegistry>) -> RpcClient {
            RpcClient::connect_over_stream(
                Arc::new(NetConfig::default()),
                uuid::Uuid::new_v4(),
                peer_addr(),
                serve(registry),
            )
            .await
            .unwrap()
        }

        fn registry_with(msg_type: MsgType, handler: Arc<dyn RpcHandler>) -> Arc<HandlerRegistry> {
            let registry = Arc::new(HandlerRegistry::new());
            registry.register(msg_type, handler);
            registry
        }

        fn lane_with(
            client: RpcClient,
            lane: Lane,
            max_streams: usize,
        ) -> crate::lane_actor::LaneHandle {
            let config = Arc::new(NetConfig {
                max_streams_per_lane: max_streams,
                ..NetConfig::default()
            });
            spawn_lane_actor(lane, LaneState::Connected(client), |h| {
                ActorReconnectContext {
                    lane,
                    config,
                    local_host_id: uuid::Uuid::new_v4(),
                    peer_host: peer_addr().to_string(),
                    tls_connector: None,
                    cancelled: h.cancel_token(),
                    handle: h,
                    task_pool: TaskPool::current("test-lane"),
                }
            })
        }

        fn raw_frame(msg_type: MsgType, stream_id: u32, flags: u8, body: bytes::Bytes) -> Frame {
            let mut header = FrameHeader::new(
                msg_type,
                Lane::Data,
                stream_id,
                u32::try_from(body.len()).unwrap(),
            );
            header.flags |= flags;
            Frame { header, body }
        }

        fn assert_remote_failure(
            result: &crate::error::Result<Message>,
            want_type: MsgType,
            want_kind: RemoteFailureKind,
        ) -> String {
            match result {
                Err(NetError::RemoteHandlerFailed {
                    msg_type,
                    kind,
                    detail,
                }) if *msg_type == want_type && *kind == want_kind => detail.clone(),
                other => panic!(
                    "expected RemoteHandlerFailed({want_type:?}, {want_kind:?}), got {other:?}"
                ),
            }
        }

        /// (a) A panicking handler answers with a typed error for that
        /// stream_id, the requester's slot is released at once (not at the
        /// lane timeout), the panic is counted, and the connection keeps
        /// serving.
        #[tokio::test(start_paused = true)]
        async fn handler_panic_replies_with_typed_error_and_releases_slot() {
            let client =
                connected_client(registry_with(MsgType::Ping, Arc::new(PanicOnZero))).await;
            let panics_before = metrics::rpc_handler_panics(MsgType::Ping);
            let in_flight_before = client.in_flight.load(Ordering::Relaxed);
            let started = tokio::time::Instant::now();

            let result = client
                .send_with_timeout(ping(0), Lane::Data, Lane::Data.timeout())
                .await;

            let detail =
                assert_remote_failure(&result, MsgType::Ping, RemoteFailureKind::HandlerPanicked);
            assert!(
                detail.contains("handler exploded on nonce 0"),
                "detail: {detail}"
            );
            assert!(
                started.elapsed() < Lane::Data.timeout(),
                "the slot must be released by the error reply, not the lane timeout"
            );
            assert_eq!(client.in_flight.load(Ordering::Relaxed), in_flight_before);
            assert_eq!(client.pending_len(), 0, "stream slot leaked");
            assert!(metrics::rpc_handler_panics(MsgType::Ping) > panics_before);

            let pong = client
                .send_with_timeout(ping(1), Lane::Data, Lane::Data.timeout())
                .await
                .unwrap();
            assert!(
                matches!(pong, Message::Pong { nonce: 1, .. }),
                "got {pong:?}"
            );
        }

        /// (a, lane level) With a one-stream window, a panicked request must
        /// hand the window to the next request immediately.
        #[tokio::test(start_paused = true)]
        async fn handler_panic_releases_lane_stream_window() {
            let client =
                connected_client(registry_with(MsgType::Ping, Arc::new(PanicOnZero))).await;
            let lane = lane_with(client, Lane::Data, 1);
            let started = tokio::time::Instant::now();

            let first = lane.send(ping(0), None).await;
            assert_remote_failure(&first, MsgType::Ping, RemoteFailureKind::HandlerPanicked);
            let second = lane.send(ping(2), None).await.unwrap();

            assert!(
                matches!(second, Message::Pong { nonce: 2, .. }),
                "got {second:?}"
            );
            assert!(
                started.elapsed() < Lane::Data.timeout(),
                "the one-stream window was held until the lane timeout"
            );
            lane.shutdown().await;
        }

        /// (b) A response that fails to encode answers with a typed error.
        #[tokio::test(start_paused = true)]
        async fn response_encode_failure_replies_with_typed_error() {
            let client =
                connected_client(registry_with(MsgType::Ping, Arc::new(UnencodableResponse))).await;
            let started = tokio::time::Instant::now();

            let result = client
                .send_with_timeout(ping(5), Lane::Data, Lane::Data.timeout())
                .await;

            assert_remote_failure(
                &result,
                MsgType::Ping,
                RemoteFailureKind::ResponseEncodeFailed,
            );
            assert!(started.elapsed() < Lane::Data.timeout());
            assert_eq!(client.pending_len(), 0, "stream slot leaked");
            assert_eq!(client.in_flight.load(Ordering::Relaxed), 0);
        }

        /// A request body that fails to decode answers with a typed error for
        /// that stream_id instead of being skipped.
        #[tokio::test(start_paused = true)]
        async fn request_decode_failure_replies_with_typed_error() {
            let io = serve(registry_with(MsgType::Ping, Arc::new(EchoPingHandler)));
            let config = NetConfig::default();
            let mut framed = Framed::new(io, InternodeCodec::new(config.max_frame_body_size));
            initiate_handshake(&mut framed, &config, uuid::Uuid::new_v4())
                .await
                .unwrap();

            let truncated = bytes::Bytes::from_static(&[1, 2, 3]);
            framed
                .send(raw_frame(MsgType::Ping, 7, 0, truncated))
                .await
                .unwrap();

            let reply = tokio::time::timeout(SILENCE, framed.next())
                .await
                .expect("no reply to an undecodable request")
                .expect("connection closed")
                .expect("frame error");
            assert_ne!(reply.header.flags & FLAG_RPC_ERROR, 0, "not an error reply");
            assert_eq!(reply.header.stream_id, 7);
            assert_eq!(reply.header.msg_type, MsgType::Ping);
            let body = RpcErrorReply::decode(&mut reply.body.clone()).unwrap();
            assert_eq!(body.kind, RemoteFailureKind::RequestDecodeFailed);
        }

        /// (c) Deadline characterization: requests DO have a deadline. A peer
        /// that never answers is failed with `Timeout` at the lane's default
        /// timeout (Data = 10 s) and its stream slot is released.
        #[tokio::test(start_paused = true)]
        async fn unanswered_request_times_out_at_lane_deadline_and_releases_slot() {
            let client =
                connected_client(registry_with(MsgType::Ping, Arc::new(NeverAnswers))).await;
            let lane = lane_with(client.clone(), Lane::Data, 1);
            let started = tokio::time::Instant::now();

            let result = lane.send(ping(3), None).await;

            assert!(
                matches!(result, Err(NetError::Timeout(_))),
                "got {result:?}"
            );
            let waited = started.elapsed();
            assert!(
                waited >= Lane::Data.timeout()
                    && waited < Lane::Data.timeout() + Duration::from_secs(1),
                "expected the 10 s Data-lane deadline, waited {waited:?}"
            );
            assert_eq!(client.pending_len(), 0, "stream slot leaked after timeout");
            assert_eq!(client.in_flight.load(Ordering::Relaxed), 0);
            lane.shutdown().await;
        }

        /// The ordered-stream frames run inline on the connection's reader. A
        /// panic there must not kill the reader: later requests on the same
        /// connection are still served.
        #[tokio::test(start_paused = true)]
        async fn ordered_stream_handler_panic_keeps_connection_serving() {
            let registry = registry_with(MsgType::RangeReadStreamChunk, Arc::new(PanicOnZero));
            registry.register(MsgType::Ping, Arc::new(PanicOnZero));
            let client = connected_client(registry).await;
            let panics_before = metrics::rpc_handler_panics(MsgType::RangeReadStreamChunk);

            client
                .fire(
                    Message::RangeReadStreamChunk(bytes::Bytes::from_static(b"chunk")),
                    Lane::Bulk,
                )
                .await
                .unwrap();
            let pong = client
                .send_with_timeout(ping(4), Lane::Data, Lane::Data.timeout())
                .await;

            assert!(
                matches!(pong, Ok(Message::Pong { nonce: 4, .. })),
                "the connection reader died with the inline handler: {pong:?}"
            );
            assert!(metrics::rpc_handler_panics(MsgType::RangeReadStreamChunk) > panics_before);
        }

        /// A fire-and-forget frame has no requester waiting, so a failed
        /// handler sends nothing back.
        #[tokio::test(start_paused = true)]
        async fn fire_and_forget_failure_sends_no_reply() {
            let io = serve(registry_with(
                MsgType::RangeReadStreamChunk,
                Arc::new(PanicOnZero),
            ));
            let config = NetConfig::default();
            let mut framed = Framed::new(io, InternodeCodec::new(config.max_frame_body_size));
            initiate_handshake(&mut framed, &config, uuid::Uuid::new_v4())
                .await
                .unwrap();

            let body = bytes::Bytes::from_static(b"chunk");
            framed
                .send(raw_frame(
                    MsgType::RangeReadStreamChunk,
                    9,
                    FLAG_FIRE_AND_FORGET,
                    body,
                ))
                .await
                .unwrap();

            let next = tokio::time::timeout(SILENCE, framed.next()).await;
            assert!(
                next.is_err(),
                "unexpected frame for a fire-and-forget request: {next:?}"
            );
        }

        /// A Handshake body exactly as a build before the capability field
        /// wrote it: the current encoding minus its 4 trailing bytes.
        fn pre_capability_handshake(config: &NetConfig) -> bytes::Bytes {
            let handshake = Message::Handshake {
                cluster_name: config.cluster_name.clone(),
                host_id: uuid::Uuid::new_v4(),
                protocol_version: crate::handshake::PROTOCOL_VERSION,
                supported_compression: vec![0],
                auth_token: vec![],
                cql_broadcast: None,
                internode_broadcast: None,
                capabilities: 0,
            };
            let mut body = BytesMut::new();
            handshake.encode(&mut body).unwrap();
            let old_len = body.len() - 4;
            assert_eq!(&body[old_len..], &[0, 0, 0, 0], "capabilities must trail");
            body.truncate(old_len);
            body.freeze()
        }

        fn encoded(msg: &Message) -> bytes::Bytes {
            let mut body = BytesMut::new();
            msg.encode(&mut body).unwrap();
            body.freeze()
        }

        /// Rolling restart, old client → new server: a peer on the previous
        /// build (its Handshake has no capability field at all) is accepted,
        /// is served normally, and is never sent an error reply it could not
        /// parse — it keeps the old behaviour of failing at its lane timeout.
        #[tokio::test(start_paused = true)]
        async fn peer_without_error_reply_capability_gets_no_error_frame() {
            let io = serve(registry_with(MsgType::Ping, Arc::new(PanicOnZero)));
            let config = NetConfig::default();
            let mut framed = Framed::new(io, InternodeCodec::new(config.max_frame_body_size));
            framed
                .send(raw_frame(
                    MsgType::Handshake,
                    0,
                    0,
                    pre_capability_handshake(&config),
                ))
                .await
                .unwrap();
            let ack = framed.next().await.unwrap().unwrap();
            let ack = Message::decode(ack.header.msg_type, &mut ack.body.clone()).unwrap();
            assert!(
                matches!(ack, Message::HandshakeAck { accepted: true, .. }),
                "old peer rejected: {ack:?}"
            );

            framed
                .send(raw_frame(MsgType::Ping, 3, 0, encoded(&ping(0))))
                .await
                .unwrap();
            let next = tokio::time::timeout(SILENCE, framed.next()).await;
            assert!(
                next.is_err(),
                "pre-capability peer was sent a frame: {next:?}"
            );

            framed
                .send(raw_frame(MsgType::Ping, 4, 0, encoded(&ping(1))))
                .await
                .unwrap();
            let reply = framed.next().await.unwrap().unwrap();
            assert_eq!(reply.header.flags & FLAG_RPC_ERROR, 0);
            assert_eq!(reply.header.stream_id, 4);
            let reply = Message::decode(reply.header.msg_type, &mut reply.body.clone()).unwrap();
            assert!(
                matches!(reply, Message::Pong { nonce: 1, .. }),
                "got {reply:?}"
            );
        }

        /// Rolling restart, new client → old server. The old server's decoder
        /// stops after `internode_broadcast` and never checks for leftover
        /// bytes (origin/main `Message::decode` and `accept_handshake`), so it
        /// reads exactly the fields of the pre-capability prefix and ignores
        /// the trailing u32. It answers with the unchanged HandshakeAck, serves
        /// requests normally, and sends nothing for a failed one: the new
        /// client then falls back to its lane deadline, the pre-fix behaviour.
        #[tokio::test(start_paused = true)]
        async fn new_client_against_pre_capability_server() {
            let (client_io, server_io) = tokio::io::duplex(1 << 20);
            tokio::spawn(async move {
                let config = NetConfig::default();
                let mut framed =
                    Framed::new(server_io, InternodeCodec::new(config.max_frame_body_size));
                let hs = framed.next().await.unwrap().unwrap();
                // What an old decoder sees: the same fields as the prefix.
                let full = Message::decode(MsgType::Handshake, &mut hs.body.clone()).unwrap();
                let prefix = hs.body.slice(..hs.body.len() - 4);
                let old_view = Message::decode(MsgType::Handshake, &mut prefix.clone()).unwrap();
                let (
                    Message::Handshake {
                        host_id: a,
                        cluster_name: ca,
                        capabilities,
                        ..
                    },
                    Message::Handshake {
                        host_id: b,
                        cluster_name: cb,
                        ..
                    },
                ) = (&full, &old_view)
                else {
                    panic!("not a handshake: {full:?}");
                };
                assert_eq!((a, ca), (b, cb));
                assert_eq!(*capabilities, crate::handshake::LOCAL_CAPABILITIES);
                let ack = Message::HandshakeAck {
                    host_id: uuid::Uuid::new_v4(),
                    protocol_version: crate::handshake::PROTOCOL_VERSION,
                    chosen_compression: 0,
                    accepted: true,
                    reason: String::new(),
                    cql_broadcast: None,
                    internode_broadcast: None,
                    // An old server advertises no capabilities.
                    capabilities: 0,
                };
                framed
                    .send(raw_frame(MsgType::HandshakeAck, 0, 0, encoded(&ack)))
                    .await
                    .unwrap();
                // Old server: answer nonce != 0, send nothing when the
                // handler "fails" on nonce 0.
                while let Some(Ok(frame)) = framed.next().await {
                    let msg =
                        Message::decode(frame.header.msg_type, &mut frame.body.clone()).unwrap();
                    if let Message::Ping { nonce, .. } = msg {
                        if nonce != 0 {
                            let pong = Message::Pong {
                                nonce,
                                ping_recv_at: 0,
                                sent_at: 0,
                            };
                            let mut reply =
                                raw_frame(MsgType::Pong, frame.header.stream_id, 0, encoded(&pong));
                            reply.header.lane = frame.header.lane;
                            framed.send(reply).await.unwrap();
                        }
                    }
                }
            });
            let client = RpcClient::connect_over_stream(
                Arc::new(NetConfig::default()),
                uuid::Uuid::new_v4(),
                peer_addr(),
                client_io,
            )
            .await
            .unwrap();

            let pong = client
                .send_with_timeout(ping(7), Lane::Data, Lane::Data.timeout())
                .await
                .unwrap();
            assert!(
                matches!(pong, Message::Pong { nonce: 7, .. }),
                "got {pong:?}"
            );

            let started = tokio::time::Instant::now();
            let failed = client
                .send_with_timeout(ping(0), Lane::Data, Lane::Data.timeout())
                .await;
            assert!(
                matches!(failed, Err(NetError::Timeout(_))),
                "got {failed:?}"
            );
            assert_eq!(started.elapsed(), Lane::Data.timeout());
            assert_eq!(client.pending_len(), 0, "stream slot leaked");
        }

        /// The connection closes at the very instant the requests' deadline
        /// fires. Each request still resolves exactly once (timeout or
        /// connection-closed), the slot map empties, and the in-flight gauge
        /// returns to zero rather than going negative: the timeout path, the
        /// close drain and a reply race only for the one oneshot sender
        /// (`DashMap::remove` hands it to exactly one of them).
        #[tokio::test(start_paused = true)]
        async fn close_racing_the_deadline_releases_each_request_once() {
            let (client_io, server_io) = tokio::io::duplex(1 << 20);
            tokio::spawn(async move {
                let config = NetConfig::default();
                let mut framed =
                    Framed::new(server_io, InternodeCodec::new(config.max_frame_body_size));
                accept_handshake(&mut framed, &config, uuid::Uuid::new_v4())
                    .await
                    .unwrap();
                for _ in 0..3 {
                    let request = framed.next().await;
                    assert!(
                        matches!(request, Some(Ok(_))),
                        "missing request: {request:?}"
                    );
                }
                tokio::time::sleep(Lane::Data.timeout()).await;
                // Drop exactly as the client deadlines fire.
            });
            let client = RpcClient::connect_over_stream(
                Arc::new(NetConfig::default()),
                uuid::Uuid::new_v4(),
                peer_addr(),
                client_io,
            )
            .await
            .unwrap();

            let send =
                |nonce| client.send_with_timeout(ping(nonce), Lane::Data, Lane::Data.timeout());
            let results = tokio::join!(send(1), send(2), send(3));

            for result in [results.0, results.1, results.2] {
                assert!(
                    matches!(&result, Err(NetError::Timeout(_)))
                        || matches!(&result, Err(NetError::Protocol(m)) if m.contains("connection closed")),
                    "got {result:?}"
                );
            }
            assert_eq!(client.pending_len(), 0, "stream slot leaked");
            assert_eq!(client.in_flight.load(Ordering::Relaxed), 0);

            // The dead connection refuses new work at once instead of
            // stranding it until a deadline.
            let after = client
                .send_with_timeout(ping(4), Lane::Data, Lane::Data.timeout())
                .await;
            assert!(
                matches!(&after, Err(NetError::Protocol(m)) if m.contains("connection closed")),
                "got {after:?}"
            );
            assert_eq!(client.pending_len(), 0);
        }

        /// A connection that closes fails every request still waiting on it
        /// at once, instead of leaving each to its lane timeout.
        #[tokio::test(start_paused = true)]
        async fn connection_close_fails_pending_requests_promptly() {
            let (client_io, server_io) = tokio::io::duplex(1 << 20);
            tokio::spawn(async move {
                let config = NetConfig::default();
                let mut framed =
                    Framed::new(server_io, InternodeCodec::new(config.max_frame_body_size));
                accept_handshake(&mut framed, &config, uuid::Uuid::new_v4())
                    .await
                    .unwrap();
                let request = framed.next().await;
                assert!(
                    matches!(request, Some(Ok(_))),
                    "no request arrived: {request:?}"
                );
                // Drop the connection without answering.
            });
            let client = RpcClient::connect_over_stream(
                Arc::new(NetConfig::default()),
                uuid::Uuid::new_v4(),
                peer_addr(),
                client_io,
            )
            .await
            .unwrap();
            let started = tokio::time::Instant::now();

            let result = client
                .send_with_timeout(ping(6), Lane::Data, Lane::Data.timeout())
                .await;

            assert!(
                matches!(&result, Err(NetError::Protocol(m)) if m.contains("connection closed")),
                "got {result:?}"
            );
            assert!(started.elapsed() < Lane::Data.timeout());
            assert_eq!(client.pending_len(), 0, "stream slot leaked");
        }
    }
}
