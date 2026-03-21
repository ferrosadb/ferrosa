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

// -- Bootstrap loader tests --

#[test]
fn load_keyspaces_from_engine() {
    let (_dir, engine) = setup_engine();
    let writer = SystemTableWriter::new(Arc::clone(&engine));

    // Write two keyspaces.
    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    for name in &["ks1", "ks2"] {
        let ks = KeyspaceMetadata {
            name: name.to_string(),
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
    }

    // Load keyspaces from engine (reads from memtable).
    let loader = ferrosa_cluster::system_table_loader::SystemTableLoader::new(Arc::clone(&engine));
    let keyspace_names = loader.load_keyspace_names().unwrap();
    assert!(
        keyspace_names.contains(&"ks1".to_string()),
        "expected ks1 in {keyspace_names:?}"
    );
    assert!(
        keyspace_names.contains(&"ks2".to_string()),
        "expected ks2 in {keyspace_names:?}"
    );
}

#[test]
fn bootstrap_system_tables_registers_and_validates() {
    use ferrosa_cluster::system_table_loader::bootstrap_system_tables;

    let (_dir, engine) = setup_engine();
    let writer = SystemTableWriter::new(Arc::clone(&engine));

    // Pre-populate a keyspace.
    use ferrosa_schema::metadata::keyspace::{KeyspaceMetadata, ReplicationParams};
    let ks = KeyspaceMetadata {
        name: "existing".to_string(),
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
    engine
        .flush(&TableId::new("system_schema", "keyspaces"))
        .unwrap();

    // Raft state has "existing" + "new_ks".
    let raft_keyspaces = vec!["existing".to_string(), "new_ks".to_string()];

    let report = bootstrap_system_tables(Arc::clone(&engine), &raft_keyspaces).unwrap();

    assert!(!report.divergences.is_empty()); // "new_ks" not in SSTables
    assert_eq!(report.validated_keyspaces.len(), 2);
}

#[test]
fn bootstrap_empty_sstables_uses_raft() {
    use ferrosa_cluster::system_table_loader::bootstrap_system_tables;

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

    let raft_keyspaces = vec!["ks_from_raft".to_string()];

    let report = bootstrap_system_tables(Arc::clone(&engine), &raft_keyspaces).unwrap();

    // With no SSTables, Raft is sole authority.
    assert_eq!(report.validated_keyspaces.len(), 1);
    assert_eq!(report.validated_keyspaces[0], "ks_from_raft");
    assert!(!report.divergences.is_empty()); // "ks_from_raft" not in SSTables
}
