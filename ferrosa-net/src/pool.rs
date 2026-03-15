use std::net::SocketAddr;
use std::sync::Arc;

use uuid::Uuid;

use crate::codec::Lane;
use crate::config::NetConfig;
use crate::error::Result;
use crate::message::Message;
use crate::rpc::client::RpcClient;

/// Maintains 3 connections to a peer — one per priority lane.
pub struct PriorityPool {
    raft: RpcClient,
    data: RpcClient,
    bulk: RpcClient,
}

impl PriorityPool {
    /// Open 3 TCP connections to the peer (one per lane).
    /// Builds TLS connector once and shares it across all 3 connections.
    pub async fn connect(
        config: Arc<NetConfig>,
        local_host_id: Uuid,
        peer_addr: SocketAddr,
    ) -> Result<Self> {
        let tls_connector = crate::tls::build_tls_connector(&config)?;
        let raft = RpcClient::connect_with_tls(
            config.clone(),
            local_host_id,
            peer_addr,
            tls_connector.as_ref(),
        )
        .await?;
        let data = RpcClient::connect_with_tls(
            config.clone(),
            local_host_id,
            peer_addr,
            tls_connector.as_ref(),
        )
        .await?;
        let bulk =
            RpcClient::connect_with_tls(config, local_host_id, peer_addr, tls_connector.as_ref())
                .await?;
        Ok(Self { raft, data, bulk })
    }

    /// The peer's host_id, obtained during the handshake on the first connection.
    pub fn peer_host_id(&self) -> Uuid {
        self.raft.peer_host_id()
    }

    pub fn client(&self, lane: Lane) -> &RpcClient {
        match lane {
            Lane::Raft => &self.raft,
            Lane::Data => &self.data,
            Lane::Bulk => &self.bulk,
        }
    }

    pub async fn send(&self, msg: Message, lane: Lane) -> Result<Message> {
        self.client(lane).send(msg, lane).await
    }

    pub async fn fire(&self, msg: Message, lane: Lane) -> Result<()> {
        self.client(lane).fire(msg, lane).await
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
    use std::time::Duration;

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
    async fn pool_connects_and_sends_on_each_lane() {
        let config = NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
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

        let pool = PriorityPool::connect(Arc::new(config), uuid::Uuid::new_v4(), addr)
            .await
            .unwrap();

        for lane in [Lane::Raft, Lane::Data, Lane::Bulk] {
            let resp = pool.send(Message::Ping { nonce: 1 }, lane).await.unwrap();
            assert!(matches!(resp, Message::Pong { nonce: 1 }));
        }
    }

    #[test]
    fn lane_timeout_values() {
        assert_eq!(Lane::Raft.timeout(), Duration::from_secs(1));
        assert_eq!(Lane::Data.timeout(), Duration::from_secs(10));
        assert_eq!(Lane::Bulk.timeout(), Duration::from_secs(60));
    }
}
