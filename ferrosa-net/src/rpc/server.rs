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
        }
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
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        let addr = listener.local_addr()?;
        let _ = self.bound_addr.send(Some(addr));
        tracing::info!(%addr, "internode server listening");
        let server = self.clone();
        tokio::spawn(async move { server.accept_loop(listener, tls_acceptor).await });
        Ok(addr)
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
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
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

        // Message dispatch loop
        while let Some(frame_result) = framed.next().await {
            let frame = frame_result?;
            let msg_type = frame.header.msg_type;
            let stream_id = frame.header.stream_id;
            // Track inbound bytes.
            self.bandwidth.bytes_received.fetch_add(
                frame.body.len() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            let msg = Message::decode(msg_type, &mut frame.body.clone())?;

            if let Some(response) = self.registry.dispatch(peer_id, msg_type, msg).await {
                let mut body = bytes::BytesMut::new();
                response.encode(&mut body)?;
                let body_len = u32::try_from(body.len())
                    .map_err(|_| NetError::Protocol("response body exceeds u32::MAX".into()))?;
                // Track outbound bytes.
                self.bandwidth
                    .bytes_sent
                    .fetch_add(body_len as u64, std::sync::atomic::Ordering::Relaxed);
                let resp_frame = Frame {
                    header: FrameHeader::new(
                        response.msg_type(),
                        frame.header.lane,
                        stream_id,
                        body_len,
                    ),
                    body: body.freeze(),
                };
                framed.send(resp_frame).await?;
            }
        }

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
}
