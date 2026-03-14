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
    pub async fn connect(
        config: Arc<NetConfig>,
        local_host_id: Uuid,
        peer_addr: SocketAddr,
    ) -> Result<Self> {
        let raft = RpcClient::connect(config.clone(), local_host_id, peer_addr).await?;
        let data = RpcClient::connect(config.clone(), local_host_id, peer_addr).await?;
        let bulk = RpcClient::connect(config, local_host_id, peer_addr).await?;
        Ok(Self { raft, data, bulk })
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
        let mut config = NetConfig::default();
        config.bind_addr = "127.0.0.1:0".parse().unwrap();
        let mut registry = HandlerRegistry::new();
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
