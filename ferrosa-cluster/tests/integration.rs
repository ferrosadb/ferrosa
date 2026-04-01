use std::sync::Arc;
use uuid::Uuid;

use ferrosa_cluster::config::ClusterConfig;
use ferrosa_cluster::pair::node::PairNode;
use ferrosa_cluster::pair::PairRole;
use ferrosa_net::config::NetConfig;
use ferrosa_storage::engine::{StorageEngine, StorageEngineConfig};
use ferrosa_storage::{CommitLogConfig, CompactionConfig, TableId};

/// Create a StorageEngine with a temp directory.
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
        flush_threshold_bytes: 4096, flush_max_age_secs: 5,
        data_dir: dir.to_path_buf(),
    };
    Arc::new(StorageEngine::new(config, None).unwrap())
}

/// Create a NetConfig for testing with a random port.
fn test_net_config() -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        ..NetConfig::default()
    }
}

/// Register a test table on the storage engine so writes succeed.
fn register_test_table(storage: &StorageEngine) {
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
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

fn test_mutation() -> ferrosa_storage::Mutation {
    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey, Token};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    ferrosa_storage::Mutation {
        mutation_id: [0x80u8; 16],
        keyspace: "test_ks".to_string(),
        table: "test_tbl".to_string(),
        key: DecoratedKey {
            token: Token(42),
            key: PartitionKey::new(vec![1, 2, 3]),
        },
        rows: vec![Row {
            clustering: vec![],
            cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1000),
        }],
        timestamp: 1000,
    }
}

#[tokio::test]
async fn pair_elects_primary_by_host_id() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let id_high = Uuid::from_bytes([0xFF; 16]);
    let id_low = Uuid::from_bytes([0x00; 16]);

    let config = Arc::new(ClusterConfig::default());
    let net1 = Arc::new(test_net_config());
    let net2 = Arc::new(test_net_config());

    let storage1 = test_storage(dir1.path());
    let storage2 = test_storage(dir2.path());

    let node1 = PairNode::new(
        config.clone(),
        net1,
        id_high,
        id_low,
        "127.0.0.1:7000".parse().unwrap(),
        storage1,
    );

    let node2 = PairNode::new(
        config,
        net2,
        id_low,
        id_high,
        "127.0.0.1:7001".parse().unwrap(),
        storage2,
    );

    assert_eq!(node1.role(), PairRole::Primary);
    assert_eq!(node2.role(), PairRole::Secondary);
}

#[tokio::test]
async fn primary_write_replicates_to_secondary() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let id_primary = Uuid::from_bytes([0xFF; 16]);
    let id_secondary = Uuid::from_bytes([0x00; 16]);

    let config = Arc::new(ClusterConfig::default());
    let storage1 = test_storage(dir1.path());
    let storage2 = test_storage(dir2.path());

    register_test_table(&storage1);
    register_test_table(&storage2);

    // Start node2 (secondary) first — peer connection will be deferred.
    let net2 = Arc::new(test_net_config());
    let node2 = PairNode::new(
        config.clone(),
        net2,
        id_secondary,
        id_primary,
        "127.0.0.1:19999".parse().unwrap(),
        storage2.clone(),
    );
    let addr2 = node2.start().await.unwrap();

    // Start node1 (primary) pointing to node2's real address.
    let net1 = Arc::new(test_net_config());
    let node1 = PairNode::new(
        config,
        net1,
        id_primary,
        id_secondary,
        addr2,
        storage1.clone(),
    );
    let addr1 = node1.start().await.unwrap();

    // Now connect node2 to node1 (deferred from start).
    node2.connect_to_peer(addr1).await.unwrap();

    // Give connections time to establish.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Write on primary via coordinator.
    let mutation = test_mutation();
    node1
        .coordinator()
        .coordinate_write(&mutation)
        .await
        .unwrap();

    // Verify data exists on secondary.
    let table_id = TableId::new("test_ks", "test_tbl");
    let result = storage2.read(&table_id, &mutation.key).unwrap();
    assert!(result.is_some(), "mutation not replicated to secondary");
}

#[tokio::test]
async fn secondary_write_forwarded_to_primary() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let id_primary = Uuid::from_bytes([0xFF; 16]);
    let id_secondary = Uuid::from_bytes([0x00; 16]);

    let config = Arc::new(ClusterConfig::default());
    let storage1 = test_storage(dir1.path());
    let storage2 = test_storage(dir2.path());

    register_test_table(&storage1);
    register_test_table(&storage2);

    let net2 = Arc::new(test_net_config());
    let node2 = PairNode::new(
        config.clone(),
        net2,
        id_secondary,
        id_primary,
        "127.0.0.1:19999".parse().unwrap(),
        storage2.clone(),
    );
    let addr2 = node2.start().await.unwrap();

    let net1 = Arc::new(test_net_config());
    let node1 = PairNode::new(
        config,
        net1,
        id_primary,
        id_secondary,
        addr2,
        storage1.clone(),
    );
    let addr1 = node1.start().await.unwrap();

    node2.connect_to_peer(addr1).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Write on SECONDARY via coordinator — should forward to primary.
    let mutation = test_mutation();
    node2
        .coordinator()
        .coordinate_write(&mutation)
        .await
        .unwrap();

    // Verify data exists on primary (storage1).
    let table_id = TableId::new("test_ks", "test_tbl");
    let result = storage1.read(&table_id, &mutation.key).unwrap();
    assert!(result.is_some(), "mutation not forwarded to primary");
}

#[tokio::test]
async fn switchover_swaps_roles() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let id_primary = Uuid::from_bytes([0xFF; 16]);
    let id_secondary = Uuid::from_bytes([0x00; 16]);

    let config = Arc::new(ClusterConfig::default());
    let storage1 = test_storage(dir1.path());
    let storage2 = test_storage(dir2.path());

    register_test_table(&storage1);
    register_test_table(&storage2);

    let net2 = Arc::new(test_net_config());
    let node2 = PairNode::new(
        config.clone(),
        net2,
        id_secondary,
        id_primary,
        "127.0.0.1:19999".parse().unwrap(),
        storage2.clone(),
    );
    let addr2 = node2.start().await.unwrap();

    let net1 = Arc::new(test_net_config());
    let node1 = PairNode::new(
        config,
        net1,
        id_primary,
        id_secondary,
        addr2,
        storage1.clone(),
    );
    let addr1 = node1.start().await.unwrap();

    node2.connect_to_peer(addr1).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify initial roles.
    assert_eq!(node1.role(), PairRole::Primary);
    assert_eq!(node2.role(), PairRole::Secondary);

    // Initiate switchover from primary.
    node1.switchover().await.unwrap();

    // Verify roles swapped.
    assert_eq!(node1.role(), PairRole::Secondary);
    assert_eq!(node2.role(), PairRole::Primary);
}
