//! Integration test: system table data survives engine restart.

use std::sync::Arc;

use ferrosa_common::{DecoratedKey, PartitionKey};
use ferrosa_schema::system::persistence::{all_system_table_schemas, SystemTableMutation};
use ferrosa_storage::engine::{StorageEngine, StorageEngineConfig};
use ferrosa_storage::{CommitLogConfig, CompactionConfig, TableId};

use ferrosa_cluster::system_table_writer::SystemTableWriter;

fn make_config(dir: &std::path::Path) -> StorageEngineConfig {
    StorageEngineConfig {
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
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
    }
}

#[test]
fn system_table_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Create engine, register system tables, write DDL, flush.
    {
        let config = make_config(dir.path());
        let engine = Arc::new(StorageEngine::new(config, None).unwrap());
        for schema in all_system_table_schemas() {
            engine.register_table(schema).unwrap();
        }

        let writer = SystemTableWriter::new(Arc::clone(&engine));

        let ks = ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
            name: "persist_ks".to_string(),
            durable_writes: true,
            replication: ferrosa_schema::metadata::keyspace::ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: std::collections::HashMap::from([(
                    "replication_factor".to_string(),
                    "3".to_string(),
                )]),
            },
        };
        writer
            .apply(SystemTableMutation::KeyspaceCreated(ks))
            .unwrap();

        // Flush to disk.
        let tid = TableId::new("system_schema", "keyspaces");
        engine.flush(&tid).unwrap();
    }
    // Engine dropped -- simulates shutdown.

    // Phase 2: Re-open engine, re-register system tables, read back data.
    {
        let config = make_config(dir.path());
        let (engine, pending_mutations) = StorageEngine::open(config, None).unwrap();
        for schema in all_system_table_schemas() {
            engine.register_table(schema).unwrap();
        }
        engine.replay_mutations(pending_mutations).unwrap();

        let tid = TableId::new("system_schema", "keyspaces");
        let key = DecoratedKey::new(PartitionKey::new(b"persist_ks".to_vec()));
        let partition = engine.read(&tid, &key).unwrap();
        assert!(
            partition.is_some(),
            "system_schema.keyspaces row should survive restart"
        );
    }
}

#[test]
fn multiple_ddl_operations_survive_restart() {
    use ferrosa_schema::auth::role::RoleMetadata;
    use std::collections::HashSet;

    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Write keyspace + role, flush all.
    {
        let config = make_config(dir.path());
        let engine = Arc::new(StorageEngine::new(config, None).unwrap());
        for schema in all_system_table_schemas() {
            engine.register_table(schema).unwrap();
        }
        let writer = SystemTableWriter::new(Arc::clone(&engine));

        // Keyspace.
        let ks = ferrosa_schema::metadata::keyspace::KeyspaceMetadata {
            name: "multi_ks".to_string(),
            durable_writes: false,
            replication: ferrosa_schema::metadata::keyspace::ReplicationParams {
                strategy: "NetworkTopologyStrategy".to_string(),
                options: std::collections::HashMap::from([("dc1".to_string(), "3".to_string())]),
            },
        };
        writer
            .apply(SystemTableMutation::KeyspaceCreated(ks))
            .unwrap();

        // Role.
        let role = RoleMetadata {
            name: "persist_role".to_string(),
            is_superuser: true,
            can_login: true,
            salted_hash: Some("$argon2id$hash".to_string()),
            member_of: HashSet::new(),
        };
        writer
            .apply(SystemTableMutation::RoleCreated(role))
            .unwrap();

        // Flush all system tables.
        for (ks_name, tbl_name) in &[("system_schema", "keyspaces"), ("system_auth", "roles")] {
            let tid = TableId::new(*ks_name, *tbl_name);
            engine.flush(&tid).unwrap();
        }
    }

    // Phase 2: Re-open and verify.
    {
        let config = make_config(dir.path());
        let (engine, pending) = StorageEngine::open(config, None).unwrap();
        for schema in all_system_table_schemas() {
            engine.register_table(schema).unwrap();
        }
        engine.replay_mutations(pending).unwrap();

        // Check keyspace.
        let tid = TableId::new("system_schema", "keyspaces");
        let key = DecoratedKey::new(PartitionKey::new(b"multi_ks".to_vec()));
        let partition = engine.read(&tid, &key).unwrap();
        assert!(partition.is_some(), "keyspace should survive restart");

        // Check durable_writes cell = false (0x00).
        let p = partition.unwrap();
        if !p.rows.is_empty() {
            let dw_cell = p.rows[0].cells.iter().find(|(idx, _)| *idx == 0);
            if let Some((_, cell)) = dw_cell {
                assert_eq!(
                    cell.value.as_deref(),
                    Some(&[0x00][..]),
                    "durable_writes should be false"
                );
            }
        }

        // Check role.
        let tid = TableId::new("system_auth", "roles");
        let key = DecoratedKey::new(PartitionKey::new(b"persist_role".to_vec()));
        let partition = engine.read(&tid, &key).unwrap();
        assert!(partition.is_some(), "role should survive restart");
    }
}
