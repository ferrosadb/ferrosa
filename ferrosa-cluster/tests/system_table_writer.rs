//! Integration tests for SystemTableWriter.

use std::sync::Arc;

use ferrosa_cluster::system_table_writer::SystemTableWriter;
use ferrosa_schema::system::persistence::{all_system_table_schemas, SystemTableMutation};
use ferrosa_storage::engine::{StorageEngine, StorageEngineConfig};
use ferrosa_storage::{CommitLogConfig, CompactionConfig, TableId};

fn setup_engine() -> (tempfile::TempDir, Arc<StorageEngine>) {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageEngineConfig {
        commit_log: CommitLogConfig {
            log_dir: dir.path().to_path_buf(),
            checkpoint_dir: dir.path().to_path_buf(),
            archive: None,
            ..CommitLogConfig::default()
        },
        compaction: CompactionConfig::from_env(dir.path().join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        flush_threshold_bytes: 4096,
        data_dir: dir.path().to_path_buf(),
    };
    let engine = Arc::new(StorageEngine::new(config, None).unwrap());

    // Register all system tables.
    for schema in all_system_table_schemas() {
        engine.register_table(schema).unwrap();
    }

    (dir, engine)
}

#[test]
fn writer_persists_keyspace_creation() {
    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};

    let (_dir, engine) = setup_engine();
    let writer = SystemTableWriter::new(Arc::clone(&engine));

    let ks = KeyspaceMetadata {
        name: "test_ks".to_string(),
        durable_writes: true,
        replication: ReplicationParams {
            strategy: "SimpleStrategy".to_string(),
            options: std::collections::HashMap::from([(
                "replication_factor".to_string(),
                "1".to_string(),
            )]),
        },
    };

    writer
        .apply(SystemTableMutation::KeyspaceCreated(ks))
        .unwrap();

    // Verify the row exists in the storage engine.
    let tid = TableId::new("system_schema", "keyspaces");
    let key =
        ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(b"test_ks".to_vec()));
    let partition = engine.read(&tid, &key).unwrap();
    assert!(partition.is_some(), "keyspace row should be readable");
}

#[test]
fn writer_persists_role_creation() {
    use ferrosa_schema::auth::role::RoleMetadata;
    use std::collections::HashSet;

    let (_dir, engine) = setup_engine();
    let writer = SystemTableWriter::new(Arc::clone(&engine));

    let role = RoleMetadata {
        name: "analyst".to_string(),
        is_superuser: false,
        can_login: true,
        salted_hash: Some("$2b$hash".to_string()),
        member_of: HashSet::new(),
    };

    writer
        .apply(SystemTableMutation::RoleCreated(role))
        .unwrap();

    let tid = TableId::new("system_auth", "roles");
    let key =
        ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(b"analyst".to_vec()));
    let partition = engine.read(&tid, &key).unwrap();
    assert!(partition.is_some(), "role row should be readable");

    let p = partition.unwrap();
    assert!(!p.rows.is_empty());
    // Verify is_superuser cell (index 0) = false (0x00).
    let cells = &p.rows[0].cells;
    let su_cell = cells.iter().find(|(idx, _)| *idx == 0).unwrap();
    assert_eq!(su_cell.1.value.as_deref(), Some(&[0x00][..]));
}

#[test]
fn writer_persists_table_creation() {
    use ferrosa_schema::metadata::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
    use ferrosa_schema::metadata::table::{TableMetadata, TableParams};
    use std::collections::HashSet;

    let (_dir, engine) = setup_engine();
    let writer = SystemTableWriter::new(Arc::clone(&engine));

    let mut table = TableMetadata {
        keyspace: "my_ks".to_string(),
        name: "users".to_string(),
        id: uuid::Uuid::nil(),
        columns: indexmap::IndexMap::new(),
        partition_key: vec!["id".to_string()],
        clustering_key: vec![],
        params: TableParams::default(),
        flags: HashSet::new(),
        extensions: std::collections::HashMap::new(),
        is_system: false,
    };
    table.columns.insert(
        "id".to_string(),
        ColumnMetadata {
            name: "id".to_string(),
            kind: ColumnKind::PartitionKey,
            position: 0,
            column_type: "uuid".to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        },
    );

    writer
        .apply(SystemTableMutation::TableCreated(Box::new(table)))
        .unwrap();

    // Check system_schema.tables.
    let tables_tid = TableId::new("system_schema", "tables");
    let key =
        ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(b"my_ks".to_vec()));
    let partition = engine.read(&tables_tid, &key).unwrap();
    assert!(partition.is_some(), "tables row should exist");

    // Check system_schema.columns.
    let columns_tid = TableId::new("system_schema", "columns");
    let partition = engine.read(&columns_tid, &key).unwrap();
    assert!(partition.is_some(), "columns rows should exist");
}
