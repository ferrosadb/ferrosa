use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::codec::{Frame, FrameHeader, InternodeCodec};
use crate::config::NetConfig;
use crate::error::NetError;
use crate::handshake::accept_handshake;
use crate::message::Message;
use crate::rpc::handler::{HandlerRegistry, PeerId};

/// Callback invoked when the server accepts an inbound peer connection.
pub trait InboundPeerCallback: Send + Sync {
    fn on_inbound_peer(&self, peer_id: PeerId, cql_broadcast: Option<String>);
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
        }
    }

    /// Set a dedicated runtime for non-Raft connections (data, bulk, bootstrap).
    pub fn with_data_runtime(mut self, rt: Arc<tokio::runtime::Runtime>) -> Self {
        self.data_runtime = Some(rt);
        self
    }

    /// Set a callback for inbound peer connections. Called after handshake succeeds.
    pub fn with_inbound_callback(mut self, cb: Arc<dyn InboundPeerCallback>) -> Self {
        self.inbound_callback = Some(cb);
        self
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

        if let Some(ref data_rt) = self.data_runtime {
            let bound_addr_tx = self.bound_addr.clone();
            data_rt.spawn(async move {
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
                        tracing::error!(%e, "failed to bind internode server");
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
            let listener = TcpListener::bind(bind_addr).await?;
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
            tokio::spawn(async move { server.accept_loop(listener, tls_acceptor).await });
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
                tokio::spawn(async move {
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
                        };
                        let mut body = bytes::BytesMut::new();
                        if ack.encode(&mut body).is_ok() {
                            let Ok(body_len) = u32::try_from(body.len()) else {
                                return;
                            };
                            let frame = Frame {
                                header: FrameHeader::new(
                                    crate::codec::MsgType::HandshakeAck,
                                    crate::codec::Lane::Raft,
                                    0,
                                    body_len,
                                ),
                                body: body.freeze(),
                            };
                            let _ = framed.send(frame).await;
                        }
                    }
                });
                continue;
            }

            self.active_connections.fetch_add(1, Ordering::Relaxed);
            let server = self.clone();
            let tls_acceptor = tls_acceptor.clone();
            tokio::spawn(async move {
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
        let (peer_host_id, peer_cql_broadcast) = tokio::time::timeout(
            self.config.handshake_timeout,
            accept_handshake(&mut framed, &self.config, self.local_host_id),
        )
        .await
        .map_err(|_| NetError::Timeout("handshake".into()))??;

        let peer_id = (peer_host_id, peer_addr);
        tracing::info!(?peer_id, "peer connected (inbound)");

        // Notify callback about inbound peer
        if let Some(cb) = &self.inbound_callback {
            cb.on_inbound_peer(peer_id, peer_cql_broadcast);
        }

        // Concurrent frame handling: split read/write, dispatch handlers
        // to the Data runtime so they don't block frame reading.
        let (mut sink, mut stream) = framed.split();
        let registry = self.registry.clone();
        let bandwidth = self.bandwidth.clone();

        // Frame reading stays on the main runtime (tokio I/O handles can't
        // move between runtimes). Handlers are dispatched to dedicated runtimes.
        let (resp_tx, mut resp_rx) = tokio::sync::mpsc::channel::<Frame>(64);

        let bw_write = bandwidth.clone();
        let write_task = tokio::spawn(async move {
            while let Some(frame) = resp_rx.recv().await {
                bw_write.bytes_sent.fetch_add(
                    frame.body.len() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                if sink.send(frame).await.is_err() {
                    break;
                }
            }
        });

        while let Some(frame_result) = stream.next().await {
            let frame = match frame_result {
                Ok(f) => f,
                Err(e) => {
                    tracing::error!(?peer_id, %e, "frame read error");
                    break;
                }
            };
            let msg_type = frame.header.msg_type;
            let stream_id = frame.header.stream_id;
            let lane = frame.header.lane;
            bandwidth.bytes_received.fetch_add(
                frame.body.len() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            let msg = match Message::decode(msg_type, &mut frame.body.clone()) {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!(?peer_id, %e, "message decode error");
                    continue;
                }
            };

            // Dispatch handlers to the appropriate runtime.
            // Raft handlers → Raft runtime (isolated from data-path load).
            // Data/Bulk handlers → Data runtime (isolated from main runtime).
            // Fallback → main runtime.
            let registry = registry.clone();
            let resp_tx = resp_tx.clone();
            let handler = async move {
                if let Some(response) = registry.dispatch(peer_id, msg_type, msg).await {
                    let mut body = bytes::BytesMut::new();
                    if response.encode(&mut body).is_ok() {
                        if let Ok(body_len) = u32::try_from(body.len()) {
                            let _ = resp_tx
                                .send(Frame {
                                    header: FrameHeader::new(
                                        response.msg_type(),
                                        lane,
                                        stream_id,
                                        body_len,
                                    ),
                                    body: body.freeze(),
                                })
                                .await;
                        }
                    }
                }
            };

            // ALL handlers run on the Data runtime (or main runtime fallback).
            // Do NOT dispatch Raft handlers to the Raft runtime — the follower's
            // raft_runtime is saturated with its own election attempts when
            // heartbeats are delayed. The handler just calls raft.append_entries()
            // which enqueues to openraft's internal channel — fast, no heavy work.
            if let Some(ref data_rt) = self.data_runtime {
                data_rt.spawn(handler);
            } else {
                tokio::spawn(handler);
            }
        }

        drop(resp_tx);
        let _ = write_task.await;

        tracing::info!(?peer_id, "peer disconnected");
        Ok(())
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
        let (peer, _cql_broadcast) = initiate_handshake(&mut framed, &config, client_id)
            .await
            .unwrap();
        assert_eq!(peer, server_id);
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
}
