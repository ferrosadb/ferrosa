//! Real indexed storage and RPC histories for stale keyed-index positives.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
use ferrosa_index::IndexKey;
use ferrosa_net::codec::MsgType;
use ferrosa_net::config::NetConfig;
use ferrosa_net::message::Message;
use ferrosa_net::peer::{PeerEventListener, PeerManager};
use ferrosa_net::rpc::handler::{HandlerRegistry, PeerId, RpcHandler};
use ferrosa_net::rpc::server::RpcServer;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{StorageEngine, StorageEngineConfig, SyncStrategyConfig, TableId};
use uuid::Uuid;

use crate::consistency::ConsistencyLevel;
use crate::coordinator::{ClusterCoordinator, MutationForwardHandler};
use crate::raft::handlers::{IndexReadInPartitionHandler, ReadRequestHandler, ReadRequestPayload};
use crate::raft::{NodeInfo, NodeState};
use crate::ring::strategy::ReplicationStrategy;
use crate::ring::TokenRing;

struct NoopListener;
impl PeerEventListener for NoopListener {
    fn on_peer_connected(&self, _: PeerId) {}
    fn on_peer_disconnected(&self, _: PeerId) {}
    fn on_peer_suspected(&self, _: PeerId) {}
    fn on_peer_recovered(&self, _: Uuid) {}
    fn on_peer_failed(&self, _: Uuid) {}
}

fn peers() -> Arc<PeerManager> {
    Arc::new(PeerManager::new(
        Arc::new(NetConfig {
            data_lane_timeout: Duration::from_millis(200),
            ..NetConfig::default()
        }),
        Uuid::new_v4(),
        Arc::new(NoopListener),
    ))
}

fn table() -> TableId {
    TableId::new("index_history", "rows")
}
fn key() -> DecoratedKey {
    DecoratedKey::new(PartitionKey::new(b"partition".to_vec()))
}
fn row(value: &[u8], timestamp: i64) -> Row {
    row_for(&7i32.to_be_bytes(), value, timestamp)
}
fn row_for(clustering: &[u8], value: &[u8], timestamp: i64) -> Row {
    Row {
        clustering: clustering.to_vec(),
        cells: vec![(0, CellValue::live(value.to_vec(), timestamp))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
    }
}

struct IndexedReplica {
    storage: Arc<StorageEngine>,
    accepts_write: bool,
    whole_partition_reads: Arc<AtomicUsize>,
    row_reads: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl RpcHandler for IndexedReplica {
    async fn handle(&self, from: PeerId, message: Message) -> Option<Message> {
        match message {
            Message::MutationForward(_) if self.accepts_write => {
                MutationForwardHandler::new(self.storage.clone())
                    .handle(from, message)
                    .await
            }
            Message::IndexReadInPartitionRequest(_) => {
                IndexReadInPartitionHandler::new(self.storage.clone())
                    .handle(from, message)
                    .await
            }
            Message::ReadRequest(ref body) => {
                self.row_reads.fetch_add(1, Ordering::SeqCst);
                let request: ReadRequestPayload = bincode::deserialize(body).unwrap();
                if request.clustering.is_empty() {
                    self.whole_partition_reads.fetch_add(1, Ordering::SeqCst);
                }
                ReadRequestHandler::new(self.storage.clone())
                    .handle(from, message)
                    .await
            }
            Message::RepairWrite(body) => {
                let mutation = crate::pair::coordinator::decode_mutation(&body).unwrap();
                self.storage
                    .apply_partition_rows(&table(), &mutation.key, mutation.rows)
                    .unwrap();
                self.storage.force_commit_log_sync().unwrap();
                Some(Message::MutationAck(Bytes::new()))
            }
            _ => None,
        }
    }
}

struct History {
    _dir: tempfile::TempDir,
    engines: Vec<Arc<StorageEngine>>,
    servers: Vec<Arc<RpcServer>>,
    writer: ClusterCoordinator,
    reader: ClusterCoordinator,
    whole_partition_reads: Arc<AtomicUsize>,
    row_reads: Arc<AtomicUsize>,
    clustering: Vec<u8>,
}

impl History {
    async fn new() -> Self {
        Self::with_clustering(
            "org.apache.cassandra.db.marshal.Int32Type",
            &7i32.to_be_bytes(),
        )
        .await
    }
    async fn with_clustering(type_name: &str, clustering: &[u8]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut engines = Vec::new();
        let mut servers = Vec::new();
        let mut ring = TokenRing::new();
        let whole_partition_reads = Arc::new(AtomicUsize::new(0));
        let row_reads = Arc::new(AtomicUsize::new(0));
        for id in 1..=3u64 {
            let mut config = StorageEngineConfig::test_config(&dir.path().join(id.to_string()));
            config.commit_log.sync_strategy = SyncStrategyConfig::Batch;
            let storage = Arc::new(StorageEngine::new(config, None).unwrap());
            storage
                .register_table_with_indexes(
                    TableSchema {
                        keyspace: table().keyspace,
                        table: table().table,
                        key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                        clustering_columns: vec![ColumnDefinition {
                            name: "ck".into(),
                            type_name: type_name.into(),
                        }],
                        static_columns: vec![],
                        regular_columns: vec![ColumnDefinition {
                            name: "value".into(),
                            type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                        }],
                        extensions: Default::default(),
                    },
                    vec![("value_idx".into(), 0)],
                )
                .unwrap();
            // A previously delivered X survives on the later isolated replica.
            // The next client Y write reaches a different durable quorum.
            if id == 3 {
                storage
                    .write(&table(), &key(), row_for(clustering, b"X", 10), 10)
                    .unwrap();
            }
            let handler = Arc::new(IndexedReplica {
                storage: storage.clone(),
                accepts_write: id != 3,
                whole_partition_reads: whole_partition_reads.clone(),
                row_reads: row_reads.clone(),
            });
            let registry = Arc::new(HandlerRegistry::new());
            for kind in [
                MsgType::MutationForward,
                MsgType::IndexReadInPartitionRequest,
                MsgType::ReadRequest,
                MsgType::RepairWrite,
            ] {
                registry.register(kind, handler.clone());
            }
            let host = Uuid::new_v4();
            let server = Arc::new(RpcServer::new(
                NetConfig {
                    bind_addr: "127.0.0.1:0".parse().unwrap(),
                    ..NetConfig::default()
                },
                host,
                registry,
            ));
            let addr = server.start_and_get_addr().await.unwrap();
            ring.add_node(
                id,
                NodeInfo {
                    host_id: host,
                    addr: addr.to_string(),
                    data_center: "dc1".into(),
                    rack: format!("rack{id}"),
                    state: NodeState::Normal,
                    cql_broadcast: None,
                },
            );
            ring.assign_tokens(id, &[(id * 100) as i64]);
            engines.push(storage);
            servers.push(server);
        }
        let ring = Arc::new(ArcSwap::from_pointee(ring));
        let writer = ClusterCoordinator::new(
            ring.clone(),
            peers(),
            1,
            engines[0].clone(),
            3,
            ConsistencyLevel::Quorum,
        );
        let reader = ClusterCoordinator::new(
            ring,
            peers(),
            3,
            engines[2].clone(),
            3,
            ConsistencyLevel::Quorum,
        );
        Self {
            _dir: dir,
            engines,
            servers,
            writer,
            reader,
            whole_partition_reads,
            row_reads,
            clustering: clustering.to_vec(),
        }
    }

    async fn replace_value_at_quorum(&self) {
        let permits = self.writer.write_semaphore.available_permits();
        self.writer
            .coordinate_write_with(
                &table(),
                &key(),
                row_for(&self.clustering, b"Y", 20),
                20,
                ConsistencyLevel::Quorum,
                3,
            )
            .await
            .expect("real local+remote WAL-backed quorum ACK");
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.writer.write_semaphore.available_permits() != permits {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("drain non-ACKed stale-replica fanout");
        // Real fresh indexes carry Y and answer X with negative evidence.
        // Stale node3 keeps X; no predicate-ignoring response fixture is used.
        for fresh in &self.engines[..2] {
            assert!(fresh
                .read_by_index_in_partition(
                    &table(),
                    "value_idx",
                    &IndexKey(b"X".to_vec()),
                    key().key.as_bytes()
                )
                .unwrap()
                .is_empty());
            assert!(!fresh
                .read_by_index_in_partition(
                    &table(),
                    "value_idx",
                    &IndexKey(b"Y".to_vec()),
                    key().key.as_bytes()
                )
                .unwrap()
                .is_empty());
        }
        assert!(!self.engines[2]
            .read_by_index_in_partition(
                &table(),
                "value_idx",
                &IndexKey(b"X".to_vec()),
                key().key.as_bytes()
            )
            .unwrap()
            .is_empty());
    }

    async fn close(self) {
        for server in self.servers {
            server.shutdown(Duration::from_millis(50)).await;
        }
    }
}

#[tokio::test]
async fn keyed_index_matcher_preserves_scalar_key_encodings_and_filtered_predicates() {
    use ferrosa_index::{FilterOp, FilterPredicate, IndexType};
    let history = History::new().await;
    let storage = &history.engines[2];
    for (name, kind) in [
        ("btree", IndexType::BTree),
        ("hash", IndexType::Hash),
        ("composite", IndexType::Composite),
    ] {
        storage.add_index(&table(), name, 0, kind).unwrap();
        assert!(storage
            .index_row_matches(
                &table(),
                name,
                &IndexKey(b"Y".to_vec()),
                &key(),
                &row(b"Y", 20)
            )
            .unwrap());
        assert!(!storage
            .index_row_matches(
                &table(),
                name,
                &IndexKey(b"X".to_vec()),
                &key(),
                &row(b"Y", 20)
            )
            .unwrap());
    }
    storage
        .add_index(&table(), "phonetic", 0, IndexType::Phonetic)
        .unwrap();
    assert!(storage
        .index_row_matches(
            &table(),
            "phonetic",
            &IndexKey(b"Smith".to_vec()),
            &key(),
            &row(b"Smythe", 20)
        )
        .unwrap());
    assert!(!storage
        .index_row_matches(
            &table(),
            "phonetic",
            &IndexKey(b"Alice".to_vec()),
            &key(),
            &row(b"Smythe", 20)
        )
        .unwrap());
    storage
        .add_index_with_predicate(
            &table(),
            "filtered",
            0,
            IndexType::Filtered,
            Some(FilterPredicate::single(
                0,
                FilterOp::Eq,
                b"allowed".to_vec(),
            )),
        )
        .unwrap();
    assert!(storage
        .index_row_matches(
            &table(),
            "filtered",
            &IndexKey(b"allowed".to_vec()),
            &key(),
            &row(b"allowed", 20)
        )
        .unwrap());
    assert!(!storage
        .index_row_matches(
            &table(),
            "filtered",
            &IndexKey(b"blocked".to_vec()),
            &key(),
            &row(b"blocked", 20)
        )
        .unwrap());
    storage
        .add_clustering_index(&table(), "ck", 0, IndexType::Hash)
        .unwrap();
    assert!(storage
        .index_row_matches(
            &table(),
            "ck",
            &IndexKey(7i32.to_be_bytes().to_vec()),
            &key(),
            &row(b"Y", 20)
        )
        .unwrap());
    assert!(!storage
        .index_row_matches(
            &table(),
            "ck",
            &IndexKey(8i32.to_be_bytes().to_vec()),
            &key(),
            &row(b"Y", 20)
        )
        .unwrap());
    storage
        .add_partition_key_index(&table(), "pk", 0, IndexType::BTree)
        .unwrap();
    assert!(storage
        .index_row_matches(
            &table(),
            "pk",
            &IndexKey(key().key.as_bytes().to_vec()),
            &key(),
            &row(b"Y", 20)
        )
        .unwrap());
    storage.add_fulltext_index(&table(), "text", 0).unwrap();
    assert!(storage
        .index_row_matches(
            &table(),
            "text",
            &IndexKey(b"X".to_vec()),
            &key(),
            &row(b"X", 20)
        )
        .is_err());
    assert!(storage
        .index_row_matches(
            &table(),
            "missing",
            &IndexKey(b"X".to_vec()),
            &key(),
            &row(b"X", 20)
        )
        .is_err());
    history.close().await;
}

#[tokio::test]
async fn keyed_index_healthy_positive_quorum_needs_no_extra_row_rpcs() {
    let history = History::new().await;
    history.replace_value_at_quorum().await;
    // Complete the third replica's ordinary catch-up so this is the healthy
    // query case, while the write history above still has a real quorum ACK.
    history.engines[2]
        .write(&table(), &key(), row(b"Y", 20), 20)
        .unwrap();
    let current = history
        .reader
        .coordinate_index_read_in_partition(
            &table(),
            &key(),
            "value_idx",
            &IndexKey(b"Y".to_vec()),
            ConsistencyLevel::Quorum,
            &ReplicationStrategy::Simple {
                replication_factor: 3,
            },
        )
        .await
        .unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(
        current[0].rows[0].cells[0].1.value.as_deref(),
        Some(b"Y".as_slice())
    );
    assert_eq!(
        history.row_reads.load(Ordering::SeqCst),
        0,
        "matching positive responders already supply this row's configured certificate"
    );
    history.close().await;
}

#[tokio::test]
async fn keyed_index_empty_clustered_remote_candidate_refuses_before_row_rpc() {
    struct UnsupportedCandidate {
        reads: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl RpcHandler for UnsupportedCandidate {
        async fn handle(&self, _: PeerId, message: Message) -> Option<Message> {
            match message {
                Message::IndexReadInPartitionRequest(_) => {
                    let partition = ferrosa_sstable::types::Partition {
                        key: key(),
                        deletion: DeletionTime::LIVE,
                        static_row: None,
                        rows: vec![row_for(&[], b"X", 10)],
                    };
                    let payload = crate::raft::handlers::IndexReadResponsePayload {
                        partitions: vec![crate::raft::handlers::partition_to_wire(partition)],
                    };
                    Some(Message::IndexReadInPartitionResponse(Bytes::from(
                        bincode::serialize(&payload).unwrap(),
                    )))
                }
                Message::ReadRequest(_) => {
                    self.reads.fetch_add(1, Ordering::SeqCst);
                    None
                }
                _ => None,
            }
        }
    }
    let history = History::new().await;
    let reads = Arc::new(AtomicUsize::new(0));
    let handler = Arc::new(UnsupportedCandidate {
        reads: reads.clone(),
    });
    let registry = Arc::new(HandlerRegistry::new());
    registry.register(MsgType::IndexReadInPartitionRequest, handler.clone());
    registry.register(MsgType::ReadRequest, handler);
    let host = Uuid::new_v4();
    let server = Arc::new(RpcServer::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..NetConfig::default()
        },
        host,
        registry,
    ));
    let addr = server.start_and_get_addr().await.unwrap();
    let mut ring = TokenRing::new();
    ring.add_node(
        1,
        NodeInfo {
            host_id: host,
            addr: addr.to_string(),
            data_center: "dc1".into(),
            rack: "rack1".into(),
            state: NodeState::Normal,
            cql_broadcast: None,
        },
    );
    ring.assign_tokens(1, &[key().token.0]);
    let reader = ClusterCoordinator::new(
        Arc::new(ArcSwap::from_pointee(ring)),
        peers(),
        99,
        history.engines[2].clone(),
        1,
        ConsistencyLevel::One,
    );
    let result = reader
        .coordinate_index_read_in_partition(
            &table(),
            &key(),
            "value_idx",
            &IndexKey(b"X".to_vec()),
            ConsistencyLevel::One,
            &ReplicationStrategy::Simple {
                replication_factor: 1,
            },
        )
        .await;
    assert!(result.is_err(), "unsupported remote data must fail closed");
    assert_eq!(
        reads.load(Ordering::SeqCst),
        0,
        "never reinterpret an empty clustered candidate as an unbounded row read"
    );
    server.shutdown(Duration::from_millis(50)).await;
    history.close().await;
}

#[tokio::test]
async fn keyed_index_empty_clustered_values_remain_explicitly_unsupported() {
    let dir = tempfile::tempdir().unwrap();
    let storage = StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
    storage
        .register_table_with_indexes(
            TableSchema {
                keyspace: table().keyspace,
                table: table().table,
                key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                clustering_columns: vec![ColumnDefinition {
                    name: "ck".into(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                }],
                static_columns: vec![],
                regular_columns: vec![ColumnDefinition {
                    name: "value".into(),
                    type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
                }],
                extensions: Default::default(),
            },
            vec![("value_idx".into(), 0)],
        )
        .unwrap();
    storage
        .add_clustering_index(&table(), "ck_idx", 0, ferrosa_index::IndexType::BTree)
        .unwrap();
    let unsupported = row_for(&[], b"X", 10);
    assert!(
        storage
            .write(&table(), &key(), unsupported.clone(), 10)
            .is_err(),
        "the existing writer contract does not accept empty clustered values"
    );
    assert!(storage
        .index_row_matches(&table(), "ck_idx", &IndexKey(vec![]), &key(), &unsupported)
        .is_err());
}

#[tokio::test]
async fn keyed_index_geo_verification_uses_the_backend_cell_key_encoding() {
    let history = History::new().await;
    let storage = &history.engines[2];
    let geo = TableId::new("index_history", "geo_rows");
    let mut schema = storage.table_schema(&table()).unwrap();
    schema.table = geo.table.clone();
    schema.regular_columns[0].type_name = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.DoubleType,org.apache.cassandra.db.marshal.DoubleType))".into();
    storage.register_table(schema).unwrap();
    storage
        .add_index(&geo, "location", 0, ferrosa_index::IndexType::Geo)
        .unwrap();
    let encode_point = |lat: f64, lon: f64| {
        let mut bytes = Vec::with_capacity(24);
        for coordinate in [lat, lon] {
            bytes.extend_from_slice(&8i32.to_be_bytes());
            bytes.extend_from_slice(&coordinate.to_be_bytes());
        }
        bytes
    };
    let point = encode_point(1.0, 1.0);
    let current = row(&point, 20);
    storage.write(&geo, &key(), current.clone(), 20).unwrap();
    assert_eq!(
        storage
            .read_by_index_in_partition(
                &geo,
                "location",
                &IndexKey(point.clone()),
                key().key.as_bytes()
            )
            .unwrap()
            .len(),
        1
    );
    assert!(storage
        .index_row_matches(&geo, "location", &IndexKey(point), &key(), &current)
        .unwrap());
    assert!(!storage
        .index_row_matches(
            &geo,
            "location",
            &IndexKey(encode_point(50.0, 50.0)),
            &key(),
            &current
        )
        .unwrap());
    history.close().await;
}

#[tokio::test]
async fn keyed_index_local_postings_do_not_resurrect_tombstones_or_expired_cells() {
    let history = History::new().await;
    let storage = &history.engines[2];
    let mut deleted = row(b"X", 20);
    deleted.cells[0].1 = CellValue::tombstone(20, 1);
    storage.write(&table(), &key(), deleted, 20).unwrap();
    assert!(storage
        .read_by_index_in_partition(
            &table(),
            "value_idx",
            &IndexKey(b"X".to_vec()),
            key().key.as_bytes()
        )
        .unwrap()
        .is_empty());
    let mut expired = row(b"X", 30);
    expired.cells[0].1 = CellValue::expiring(b"X".to_vec(), 30, 1, 1);
    storage.write(&table(), &key(), expired, 30).unwrap();
    assert!(storage
        .read_by_index_in_partition(
            &table(),
            "value_idx",
            &IndexKey(b"X".to_vec()),
            key().key.as_bytes()
        )
        .unwrap()
        .is_empty());
    storage.write(&table(), &key(), row(b"X", 40), 40).unwrap();
    assert_eq!(
        storage
            .read_by_index_in_partition(
                &table(),
                "value_idx",
                &IndexKey(b"X".to_vec()),
                key().key.as_bytes()
            )
            .unwrap()
            .len(),
        1
    );
    history.close().await;
}

#[tokio::test]
async fn keyed_index_local_value_change_filters_stale_insert_only_postings() {
    let history = History::new().await;
    history.engines[2]
        .write(&table(), &key(), row(b"Y", 20), 20)
        .unwrap();
    let old = history.engines[2]
        .read_by_index_in_partition(
            &table(),
            "value_idx",
            &IndexKey(b"X".to_vec()),
            key().key.as_bytes(),
        )
        .unwrap();
    assert!(
        old.is_empty(),
        "a stale X posting must not return the canonical Y row: {old:?}"
    );
    let current = history.engines[2]
        .read_by_index_in_partition(
            &table(),
            "value_idx",
            &IndexKey(b"Y".to_vec()),
            key().key.as_bytes(),
        )
        .unwrap();
    assert_eq!(current.len(), 1);
    history.close().await;
}

#[tokio::test]
async fn keyed_index_acknowledged_value_change_cannot_leave_a_stale_positive() {
    let history = History::new().await;
    history.replace_value_at_quorum().await;
    let result = history
        .reader
        .coordinate_index_read_in_partition(
            &table(),
            &key(),
            "value_idx",
            &IndexKey(b"X".to_vec()),
            ConsistencyLevel::Quorum,
            &ReplicationStrategy::Simple {
                replication_factor: 3,
            },
        )
        .await
        .expect("configured quorum answers the keyed predicate");
    assert!(
        result.is_empty(),
        "fresh quorum negative evidence must invalidate stale X after ACKed X->Y: {result:?}"
    );
    let canonical_reads = history.row_reads.load(Ordering::SeqCst);
    assert!(
        canonical_reads > 0,
        "an ambiguous stale positive must obtain canonical row evidence"
    );
    let current = history
        .reader
        .coordinate_index_read_in_partition(
            &table(),
            &key(),
            "value_idx",
            &IndexKey(b"Y".to_vec()),
            ConsistencyLevel::Quorum,
            &ReplicationStrategy::Simple {
                replication_factor: 3,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        current.len(),
        1,
        "canonical Y remains queryable after old X is suppressed"
    );
    assert_eq!(
        current[0].rows[0].cells[0].1.value.as_deref(),
        Some(b"Y".as_slice())
    );
    assert_eq!(
        history.whole_partition_reads.load(Ordering::SeqCst),
        0,
        "verification must use exact clustering reads"
    );
    history.close().await;
}
