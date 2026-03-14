use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

use crate::codec::{Frame, FrameHeader, InternodeCodec};
use crate::config::NetConfig;
use crate::error::NetError;
use crate::handshake::accept_handshake;
use crate::message::Message;
use crate::rpc::handler::HandlerRegistry;

pub struct RpcServer {
    config: Arc<NetConfig>,
    local_host_id: uuid::Uuid,
    registry: Arc<HandlerRegistry>,
    active_connections: Arc<AtomicUsize>,
    bound_addr: tokio::sync::watch::Sender<Option<std::net::SocketAddr>>,
    #[allow(dead_code)]
    bound_addr_rx: tokio::sync::watch::Receiver<Option<std::net::SocketAddr>>,
}

impl RpcServer {
    pub fn new(config: NetConfig, local_host_id: uuid::Uuid, registry: HandlerRegistry) -> Self {
        let (bound_addr, bound_addr_rx) = tokio::sync::watch::channel(None);
        Self {
            config: Arc::new(config),
            local_host_id,
            registry: Arc::new(registry),
            active_connections: Arc::new(AtomicUsize::new(0)),
            bound_addr,
            bound_addr_rx,
        }
    }

    pub async fn start_and_get_addr(
        self: &Arc<Self>,
    ) -> crate::error::Result<std::net::SocketAddr> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        let addr = listener.local_addr()?;
        let _ = self.bound_addr.send(Some(addr));
        tracing::info!(%addr, "internode server listening");
        let server = self.clone();
        tokio::spawn(async move { server.accept_loop(listener).await });
        Ok(addr)
    }

    async fn accept_loop(self: Arc<Self>, listener: TcpListener) {
        loop {
            let (stream, peer_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::error!(error = %e, "accept error");
                    continue;
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
            tokio::spawn(async move {
                if let Err(e) = server.handle_connection(stream, peer_addr).await {
                    tracing::error!(%peer_addr, error = %e, "connection error");
                }
                server.active_connections.fetch_sub(1, Ordering::Relaxed);
            });
        }
    }

    async fn handle_connection(
        &self,
        stream: tokio::net::TcpStream,
        peer_addr: std::net::SocketAddr,
    ) -> crate::error::Result<()> {
        let mut framed = Framed::new(stream, InternodeCodec::new(self.config.max_frame_body_size));

        // Handshake with timeout (T5)
        let peer_host_id = tokio::time::timeout(
            self.config.handshake_timeout,
            accept_handshake(&mut framed, &self.config, self.local_host_id),
        )
        .await
        .map_err(|_| NetError::Timeout("handshake".into()))??;

        let peer_id = (peer_host_id, peer_addr);
        tracing::info!(?peer_id, "peer connected");

        // Message dispatch loop
        while let Some(frame_result) = framed.next().await {
            let frame = frame_result?;
            let msg_type = frame.header.msg_type;
            let stream_id = frame.header.stream_id;
            let msg = Message::decode(msg_type, &mut frame.body.clone())?;

            if let Some(response) = self.registry.dispatch(peer_id, msg_type, msg).await {
                let mut body = bytes::BytesMut::new();
                response.encode(&mut body)?;
                let body_len = u32::try_from(body.len())
                    .map_err(|_| NetError::Protocol("response body exceeds u32::MAX".into()))?;
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
                Message::Ping { nonce } => Some(Message::Pong { nonce }),
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
        let registry = HandlerRegistry::new();
        let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));

        let addr = server.start_and_get_addr().await.unwrap();
        let client_id = uuid::Uuid::new_v4();
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut framed = Framed::new(stream, InternodeCodec::new(config.max_frame_body_size));
        let peer = initiate_handshake(&mut framed, &config, client_id)
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
        let registry = HandlerRegistry::new();
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
        let mut registry = HandlerRegistry::new();
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
        let ping = Message::Ping { nonce: 42 };
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
        assert!(matches!(resp, Message::Pong { nonce: 42 }));
    }
}
