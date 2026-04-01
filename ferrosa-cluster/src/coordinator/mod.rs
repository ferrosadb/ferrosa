//! Cluster coordinator -- fans out writes and reads to replicas
//! with tunable consistency level enforcement.

pub mod batch;
pub mod metrics;
pub mod read;
pub mod write;

use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;

use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};
use ferrosa_storage::engine::StorageEngine;
use ferrosa_storage::TableId;

use crate::consistency::ConsistencyLevel;
use crate::coordinator::metrics::ReadRepairMetrics;
use crate::hints::HintStore;
use crate::pair::coordinator::decode_mutation;
use crate::raft::state_machine::RaftState;
use crate::ring::TokenRing;

/// Coordinates writes and reads across replicas in cluster mode.
pub struct ClusterCoordinator {
    pub(crate) ring: Arc<ArcSwap<TokenRing>>,
    pub(crate) peer_manager: Arc<PeerManager>,
    pub(crate) local_node_id: u64,
    pub(crate) storage: Arc<StorageEngine>,
    pub(crate) default_rf: usize,
    pub(crate) default_cl: ConsistencyLevel,
    /// Optional hint store — when `Some`, failed remote replicas receive hints
    /// after a successful quorum write.  When `None` (e.g. in unit tests),
    /// hint storage is skipped.
    pub(crate) hint_store: Option<Arc<HintStore>>,
    /// Read repair metrics (attempted/succeeded/failed counters).
    pub repair_metrics: Arc<ReadRepairMetrics>,
    /// Optional snapshot of Raft state for index-aware replica selection.
    pub(crate) raft_state: Option<Arc<ArcSwap<RaftState>>>,
}

impl ClusterCoordinator {
    pub fn new(
        ring: Arc<ArcSwap<TokenRing>>,
        peer_manager: Arc<PeerManager>,
        local_node_id: u64,
        storage: Arc<StorageEngine>,
        default_rf: usize,
        default_cl: ConsistencyLevel,
    ) -> Self {
        Self {
            ring,
            peer_manager,
            local_node_id,
            storage,
            default_rf,
            default_cl,
            hint_store: None,
            repair_metrics: Arc::new(ReadRepairMetrics::new()),
            raft_state: None,
        }
    }

    /// Attach a hint store to this coordinator.
    pub fn with_hint_store(mut self, hint_store: Arc<HintStore>) -> Self {
        self.hint_store = Some(hint_store);
        self
    }

    /// Return the data center of this coordinator's local node.
    ///
    /// Looks up `local_node_id` in the current token ring snapshot.
    /// Returns `None` if the local node is not (yet) registered.
    pub fn local_dc(&self) -> Option<String> {
        let ring = self.ring.load();
        ring.get_node(self.local_node_id)
            .map(|info| info.data_center.clone())
    }

    /// Attach a Raft state snapshot for index-aware replica selection.
    pub fn with_raft_state(mut self, state: Arc<ArcSwap<RaftState>>) -> Self {
        self.raft_state = Some(state);
        self
    }
}

// ---------------------------------------------------------------------------
// MutationForwardHandler — receives MutationForward RPCs from coordinators
// ---------------------------------------------------------------------------

/// RPC handler that receives `MutationForward` messages from coordinators
/// and applies them to local storage.
pub struct MutationForwardHandler {
    storage: Arc<StorageEngine>,
}

impl MutationForwardHandler {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl RpcHandler for MutationForwardHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::MutationForward(b) => b,
            _ => return None,
        };
        let mutation = decode_mutation(&body).ok()?;
        let table_id = TableId::new(&mutation.keyspace, &mutation.table);
        for row in &mutation.rows {
            let _ = self
                .storage
                .write(&table_id, &mutation.key, row.clone(), mutation.timestamp);
        }
        Some(Message::MutationAck(Bytes::new()))
    }
}

// ---------------------------------------------------------------------------
// RepairWriteHandler — receives RepairWrite RPCs from coordinators
// ---------------------------------------------------------------------------

/// RPC handler that receives `RepairWrite` messages from coordinators
/// performing inline read repair. Applies the mutation directly to local
/// storage without forwarding to other replicas (prevents cascade).
pub struct RepairWriteHandler {
    storage: Arc<StorageEngine>,
    metrics: Arc<metrics::ReadRepairMetrics>,
}

impl RepairWriteHandler {
    pub fn new(storage: Arc<StorageEngine>, metrics: Arc<metrics::ReadRepairMetrics>) -> Self {
        Self { storage, metrics }
    }
}

#[async_trait::async_trait]
impl RpcHandler for RepairWriteHandler {
    async fn handle(&self, _from: PeerId, msg: Message) -> Option<Message> {
        let body = match msg {
            Message::RepairWrite(b) => b,
            _ => return None,
        };
        let mutation = match decode_mutation(&body) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("RepairWriteHandler: failed to decode mutation: {e}");
                self.metrics.inc_failed();
                return None;
            }
        };
        let table_id = TableId::new(&mutation.keyspace, &mutation.table);
        for row in &mutation.rows {
            if let Err(e) =
                self.storage
                    .write(&table_id, &mutation.key, row.clone(), mutation.timestamp)
            {
                tracing::warn!("RepairWriteHandler: storage write failed: {e}");
                self.metrics.inc_failed();
                return None;
            }
        }
        self.metrics.inc_succeeded();
        // Fire-and-forget: no response.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ClusterError;
    use crate::pair::coordinator::encode_mutation;
    use crate::raft::{NodeInfo, NodeState};
    use crate::ring::TokenRing;
    use ferrosa_common::key::DecoratedKey;
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    use ferrosa_common::{CellValue, PartitionKey, Token};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
    use ferrosa_storage::{CommitLogConfig, CompactionConfig, Mutation, StorageEngineConfig};
    use uuid::Uuid;

    fn test_storage(dir: &std::path::Path) -> Arc<StorageEngine> {
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                log_dir: dir.to_path_buf(),
                checkpoint_dir: dir.to_path_buf(),
                archive: None,
                ..CommitLogConfig::default()
            },
            compaction: CompactionConfig::from_env(dir.join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            flush_threshold_bytes: 4096,
            flush_max_age_secs: 5,
            data_dir: dir.to_path_buf(),
        };
        Arc::new(StorageEngine::new(config, None).unwrap())
    }

    fn make_node(addr: &str) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
        }
    }

    fn test_key() -> DecoratedKey {
        DecoratedKey {
            token: Token(42),
            key: PartitionKey::new(vec![1, 2, 3]),
        }
    }

    fn test_row() -> Row {
        Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        }
    }

    fn register_test_table(storage: &StorageEngine) {
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "val".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        };
        storage.register_table(schema).unwrap();
    }

    #[test]
    fn coordinator_has_repair_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());

        let local_node_id = 1u64;
        let ring = TokenRing::new();

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            local_node_id,
            storage,
            1,
            ConsistencyLevel::One,
        );

        // Metrics should start at zero.
        let text = coordinator.repair_metrics.to_prometheus_text();
        assert!(text.contains("ferrosa_read_repairs_attempted_total 0"));
    }

    #[tokio::test]
    async fn coordinate_write_local_replica_writes_to_storage() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            // PeerManager won't be used since we're the local replica
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            local_node_id,
            storage.clone(),
            1, // RF=1
            ConsistencyLevel::One,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row();

        coordinator
            .coordinate_write(&table_id, &key, row, 1000)
            .await
            .unwrap();

        // Verify the write landed in storage
        let result = storage.read(&table_id, &key).unwrap();
        assert!(result.is_some(), "local write should be readable");
    }

    #[tokio::test]
    async fn coordinate_write_unavailable_when_too_few_replicas() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let ring = TokenRing::new(); // empty ring, no replicas

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            local_node_id,
            storage.clone(),
            3, // RF=3
            ConsistencyLevel::Quorum,
        );

        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row();

        let result = coordinator
            .coordinate_write(&table_id, &key, row, 1000)
            .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            ClusterError::Unavailable {
                required, alive, ..
            } => {
                assert_eq!(required, 2); // QUORUM of 3 = 2
                assert_eq!(alive, 0);
            }
            other => panic!("expected Unavailable, got: {other}"),
        }
    }

    #[tokio::test]
    async fn coordinate_read_local_replica_reads_from_storage() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        ring.add_node(local_node_id, make_node("10.0.0.1:7000"));
        ring.assign_tokens(local_node_id, &[0, 100, 200]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            local_node_id,
            storage.clone(),
            1,
            ConsistencyLevel::One,
        );

        // Write directly to storage first
        let table_id = TableId::new("test_ks", "test_tbl");
        let key = test_key();
        let row = test_row();
        storage.write(&table_id, &key, row.clone(), 1000).unwrap();

        // Read via coordinator
        let result = coordinator.coordinate_read(&table_id, &key).await.unwrap();
        assert!(result.is_some(), "should read back written data");
        let rows = result.unwrap();
        assert!(!rows.is_empty());
    }

    #[tokio::test]
    async fn mutation_forward_handler_applies_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let handler = MutationForwardHandler::new(storage.clone());

        let mutation = Mutation {
            mutation_id: [0x90u8; 16],
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key: test_key(),
            rows: vec![test_row()],
            timestamp: 1000,
        };
        let body = encode_mutation(&mutation);
        let msg = Message::MutationForward(body);

        let peer_id = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let response = handler.handle(peer_id, msg).await;

        assert!(matches!(response, Some(Message::MutationAck(_))));

        // Verify the mutation was applied
        let table_id = TableId::new("test_ks", "test_tbl");
        let result = storage.read(&table_id, &test_key()).unwrap();
        assert!(result.is_some());
    }

    /// No-op listener for tests that don't care about peer events.
    struct NoopListener;
    impl ferrosa_net::peer::PeerEventListener for NoopListener {
        fn on_peer_connected(&self, _peer: PeerId) {}
        fn on_peer_disconnected(&self, _peer: PeerId) {}
        fn on_peer_suspected(&self, _peer: PeerId) {}
        fn on_peer_recovered(&self, _peer_id: uuid::Uuid) {}
        fn on_peer_failed(&self, _peer_id: uuid::Uuid) {}
    }

    #[test]
    fn local_dc_returns_coordinator_datacenter() {
        let local_node_id = 1u64;
        let mut ring = TokenRing::new();
        let mut node = make_node("10.0.0.1:7000");
        node.data_center = "us-east-1".to_string();
        ring.add_node(local_node_id, node);
        ring.assign_tokens(local_node_id, &[0]);

        let coordinator = ClusterCoordinator::new(
            Arc::new(ArcSwap::from_pointee(ring)),
            Arc::new(PeerManager::new(
                Arc::new(ferrosa_net::config::NetConfig::default()),
                Uuid::new_v4(),
                Arc::new(NoopListener),
            )),
            local_node_id,
            test_storage(tempfile::tempdir().unwrap().path()),
            1,
            ConsistencyLevel::One,
        );

        assert_eq!(coordinator.local_dc(), Some("us-east-1".to_string()));
    }

    #[tokio::test]
    async fn repair_write_handler_applies_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        register_test_table(&storage);

        let metrics = Arc::new(super::metrics::ReadRepairMetrics::new());
        let handler = super::RepairWriteHandler::new(storage.clone(), metrics.clone());

        let mutation = Mutation {
            mutation_id: [0x91u8; 16],
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            key: test_key(),
            rows: vec![test_row()],
            timestamp: 1000,
        };
        let body = encode_mutation(&mutation);
        let msg = Message::RepairWrite(body);

        let peer_id = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let response = handler.handle(peer_id, msg).await;

        // Fire-and-forget: no response expected.
        assert!(response.is_none(), "RepairWrite is fire-and-forget");

        // Verify the mutation was applied to storage.
        let table_id = TableId::new("test_ks", "test_tbl");
        let result = storage.read(&table_id, &test_key()).unwrap();
        assert!(result.is_some(), "repair write should land in storage");

        // Metrics should show one success.
        assert_eq!(
            metrics
                .read_repairs_succeeded
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn repair_write_handler_ignores_wrong_message_type() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let metrics = Arc::new(super::metrics::ReadRepairMetrics::new());
        let handler = super::RepairWriteHandler::new(storage, metrics);

        let peer_id = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let msg = Message::Ping {
            nonce: 42,
            sent_at: 0,
        };
        let response = handler.handle(peer_id, msg).await;
        assert!(response.is_none());
    }

    #[tokio::test]
    async fn repair_write_handler_corrupt_body_increments_failed() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(dir.path());
        let metrics = Arc::new(super::metrics::ReadRepairMetrics::new());
        let handler = super::RepairWriteHandler::new(storage, metrics.clone());

        let peer_id = (Uuid::new_v4(), "127.0.0.1:7000".parse().unwrap());
        let msg = Message::RepairWrite(Bytes::from_static(b"garbage"));
        let response = handler.handle(peer_id, msg).await;
        assert!(response.is_none());

        assert_eq!(
            metrics
                .read_repairs_failed
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }
}
