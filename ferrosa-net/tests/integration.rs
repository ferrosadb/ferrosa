use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use ferrosa_net::codec::{Lane, MsgType};
use ferrosa_net::config::NetConfig;
use ferrosa_net::message::Message;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::server::RpcServer;
use ferrosa_net::rpc::{HandlerRegistry, PeerId, RpcHandler};

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

struct TestListener {
    connected: AtomicBool,
}

impl PeerEventListener for TestListener {
    fn on_peer_connected(&self, _peer: PeerId) {
        self.connected.store(true, Ordering::Relaxed);
    }
    fn on_peer_disconnected(&self, _peer: PeerId) {}
    fn on_peer_suspected(&self, _peer: PeerId) {}
    fn on_peer_recovered(&self, _peer_id: uuid::Uuid) {}
    fn on_peer_failed(&self, _peer_id: uuid::Uuid) {}
}

#[tokio::test]
async fn two_peers_handshake_and_exchange_messages() {
    let config = NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        ..NetConfig::default()
    };

    let server_id = uuid::Uuid::new_v4();
    let registry = Arc::new(HandlerRegistry::new());
    registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
    let server = Arc::new(RpcServer::new(config.clone(), server_id, registry));

    let addr = server.start_and_get_addr().await.unwrap();

    // Connect client via PriorityPool (tests all 3 lanes)
    let client_id = uuid::Uuid::new_v4();
    let listener = Arc::new(TestListener {
        connected: AtomicBool::new(false),
    });
    let pm = Arc::new(PeerManager::new(
        Arc::new(config.clone()),
        client_id,
        listener.clone(),
    ));

    let pool =
        ferrosa_net::pool::PriorityPool::connect(Arc::new(config), client_id, &addr.to_string())
            .await
            .unwrap();

    let peer_id = (server_id, addr);
    pm.add_peer(peer_id, pool).await;
    assert!(listener.connected.load(Ordering::Relaxed));

    // Send Ping on data lane, receive Pong
    let resp = pm
        .send(
            server_id,
            Message::Ping {
                nonce: 42,
                sent_at: 0,
            },
            Lane::Data,
        )
        .await
        .unwrap();
    assert!(matches!(resp, Message::Pong { nonce: 42, .. }));
}

#[tokio::test]
async fn two_peers_with_psk_authentication() {
    let config = NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        psk: Some("test-secret".to_string()),
        ..NetConfig::default()
    };

    let registry = Arc::new(HandlerRegistry::new());
    registry.register(MsgType::Ping, Arc::new(EchoPingHandler));
    let server = Arc::new(RpcServer::new(
        config.clone(),
        uuid::Uuid::new_v4(),
        registry,
    ));
    let addr = server.start_and_get_addr().await.unwrap();

    // Client with same PSK should connect successfully
    let client =
        ferrosa_net::rpc::client::RpcClient::connect(Arc::new(config), uuid::Uuid::new_v4(), addr)
            .await
            .unwrap();

    let resp = client
        .send(
            Message::Ping {
                nonce: 7,
                sent_at: 0,
            },
            Lane::Raft,
        )
        .await
        .unwrap();
    assert!(matches!(resp, Message::Pong { nonce: 7, .. }));
}

#[tokio::test]
async fn psk_mismatch_rejects_connection() {
    let server_config = NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        psk: Some("secret-a".to_string()),
        ..NetConfig::default()
    };

    let registry = Arc::new(HandlerRegistry::new());
    let server = Arc::new(RpcServer::new(
        server_config.clone(),
        uuid::Uuid::new_v4(),
        registry,
    ));
    let addr = server.start_and_get_addr().await.unwrap();

    // Client with different PSK — handshake should fail
    let client_config = NetConfig {
        psk: Some("secret-b".to_string()),
        ..server_config.clone()
    };
    let result = ferrosa_net::rpc::client::RpcClient::connect(
        Arc::new(client_config),
        uuid::Uuid::new_v4(),
        addr,
    )
    .await;
    assert!(matches!(
        result,
        Err(ferrosa_net::error::NetError::HandshakeFailed(_))
    ));
}
