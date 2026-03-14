use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::codec::Framed;

use crate::codec::{Frame, FrameHeader, InternodeCodec, Lane, FLAG_FIRE_AND_FORGET};
use crate::config::NetConfig;
use crate::error::{NetError, Result};
use crate::handshake::initiate_handshake;
use crate::message::Message;

pub struct RpcClient {
    #[allow(dead_code)] // used in future phases (reconnection, pool management)
    config: Arc<NetConfig>,
    peer_addr: std::net::SocketAddr,
    peer_host_id: uuid::Uuid,
    pending: Arc<Mutex<HashMap<u32, oneshot::Sender<Message>>>>,
    tx: mpsc::Sender<Frame>,
    next_stream_id: Arc<AtomicU32>,
}

impl RpcClient {
    pub fn peer_addr(&self) -> std::net::SocketAddr {
        self.peer_addr
    }

    /// The peer's host_id, obtained during the handshake.
    pub fn peer_host_id(&self) -> uuid::Uuid {
        self.peer_host_id
    }

    pub async fn connect(
        config: Arc<NetConfig>,
        local_host_id: uuid::Uuid,
        peer_addr: std::net::SocketAddr,
    ) -> Result<Self> {
        let tls_connector = crate::tls::build_tls_connector(&config)?;
        Self::connect_with_tls(config, local_host_id, peer_addr, tls_connector.as_ref()).await
    }

    /// Connect with an optional pre-built TLS connector (avoids rebuilding per lane).
    pub async fn connect_with_tls(
        config: Arc<NetConfig>,
        local_host_id: uuid::Uuid,
        peer_addr: std::net::SocketAddr,
        tls_connector: Option<&tokio_rustls::TlsConnector>,
    ) -> Result<Self> {
        let tcp_stream = TcpStream::connect(peer_addr).await?;

        // Perform the protocol handshake (and TLS if configured) using a helper
        // that handles the stream type generically.
        if let Some(connector) = tls_connector {
            let domain = rustls::pki_types::ServerName::try_from(peer_addr.ip().to_string())
                .map_err(|e| NetError::Protocol(format!("invalid TLS server name: {e}")))?;
            let tls_stream = connector.connect(domain, tcp_stream).await.map_err(|e| {
                NetError::Protocol(format!("TLS connect to {peer_addr} failed: {e}"))
            })?;
            let framed = Framed::new(tls_stream, InternodeCodec::new(config.max_frame_body_size));
            Self::finish_connect(config, local_host_id, peer_addr, framed).await
        } else {
            let framed = Framed::new(tcp_stream, InternodeCodec::new(config.max_frame_body_size));
            Self::finish_connect(config, local_host_id, peer_addr, framed).await
        }
    }

    async fn finish_connect<S>(
        config: Arc<NetConfig>,
        local_host_id: uuid::Uuid,
        peer_addr: std::net::SocketAddr,
        mut framed: Framed<S, InternodeCodec>,
    ) -> Result<Self>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let peer_host_id = tokio::time::timeout(
            config.handshake_timeout,
            initiate_handshake(&mut framed, &config, local_host_id),
        )
        .await
        .map_err(|_| NetError::Timeout("handshake".into()))??;

        let pending: Arc<Mutex<HashMap<u32, oneshot::Sender<Message>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (tx, mut rx) = mpsc::channel::<Frame>(256);

        let (mut sink, mut stream) = framed.split();

        // Write loop
        tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                if sink.send(frame).await.is_err() {
                    break;
                }
            }
        });

        // Read loop
        let pending_clone = pending.clone();
        tokio::spawn(async move {
            while let Some(Ok(frame)) = stream.next().await {
                let stream_id = frame.header.stream_id;
                if let Ok(msg) = Message::decode(frame.header.msg_type, &mut frame.body.clone()) {
                    let mut map = pending_clone.lock().await;
                    if let Some(sender) = map.remove(&stream_id) {
                        let _ = sender.send(msg);
                    }
                }
            }
        });

        Ok(Self {
            config,
            peer_addr,
            peer_host_id,
            pending,
            tx,
            next_stream_id: Arc::new(AtomicU32::new(1)),
        })
    }

    pub async fn send(&self, msg: Message, lane: Lane) -> Result<Message> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let (resp_tx, resp_rx) = oneshot::channel();

        self.pending.lock().await.insert(stream_id, resp_tx);

        let mut body = BytesMut::new();
        msg.encode(&mut body)?;
        let body_len = u32::try_from(body.len())
            .map_err(|_| NetError::Protocol("request body exceeds u32::MAX".into()))?;
        let frame = Frame {
            header: FrameHeader::new(msg.msg_type(), lane, stream_id, body_len),
            body: body.freeze(),
        };
        self.tx
            .send(frame)
            .await
            .map_err(|_| NetError::Protocol("connection closed".into()))?;

        tokio::time::timeout(lane.timeout(), resp_rx)
            .await
            .map_err(|_| NetError::Timeout(format!("{:?} lane timeout", lane)))?
            .map_err(|_| NetError::Protocol("response channel dropped".into()))
    }

    pub async fn fire(&self, msg: Message, lane: Lane) -> Result<()> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let mut body = BytesMut::new();
        msg.encode(&mut body)?;
        let body_len = u32::try_from(body.len())
            .map_err(|_| NetError::Protocol("fire body exceeds u32::MAX".into()))?;
        let mut header = FrameHeader::new(msg.msg_type(), lane, stream_id, body_len);
        header.flags |= FLAG_FIRE_AND_FORGET;
        let frame = Frame {
            header,
            body: body.freeze(),
        };
        self.tx
            .send(frame)
            .await
            .map_err(|_| NetError::Protocol("connection closed".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{Lane, MsgType};
    use crate::config::NetConfig;
    use crate::message::Message;
    use crate::rpc::handler::{HandlerRegistry, PeerId, RpcHandler};
    use crate::rpc::server::RpcServer;
    use std::sync::Arc;

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

    async fn start_echo_server(config: &NetConfig) -> std::net::SocketAddr {
        let registry = Arc::new(HandlerRegistry::new());
        registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
        let server = Arc::new(RpcServer::new(
            config.clone(),
            uuid::Uuid::new_v4(),
            registry,
        ));
        server.start_and_get_addr().await.unwrap()
    }

    #[tokio::test]
    async fn client_connects_and_handshakes() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let addr = start_echo_server(&config).await;

        let client = RpcClient::connect(Arc::new(config), uuid::Uuid::new_v4(), addr)
            .await
            .unwrap();
        assert_eq!(client.peer_addr(), addr);
    }

    #[tokio::test]
    async fn client_send_and_receive() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let addr = start_echo_server(&config).await;

        let client = RpcClient::connect(Arc::new(config), uuid::Uuid::new_v4(), addr)
            .await
            .unwrap();
        let resp = client
            .send(Message::Ping { nonce: 99 }, Lane::Raft)
            .await
            .unwrap();
        assert!(matches!(resp, Message::Pong { nonce: 99 }));
    }

    #[tokio::test]
    async fn client_fire_and_forget() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let addr = start_echo_server(&config).await;

        let client = RpcClient::connect(Arc::new(config), uuid::Uuid::new_v4(), addr)
            .await
            .unwrap();
        client
            .fire(Message::Ping { nonce: 1 }, Lane::Data)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn client_timeout_on_no_response() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        };
        let registry = Arc::new(HandlerRegistry::new()); // empty — no handlers
        let server = Arc::new(RpcServer::new(
            config.clone(),
            uuid::Uuid::new_v4(),
            registry,
        ));
        let addr = server.start_and_get_addr().await.unwrap();

        let client = RpcClient::connect(Arc::new(config), uuid::Uuid::new_v4(), addr)
            .await
            .unwrap();
        let result = client.send(Message::Ping { nonce: 1 }, Lane::Raft).await;
        assert!(matches!(result, Err(NetError::Timeout(_))));
    }
}
