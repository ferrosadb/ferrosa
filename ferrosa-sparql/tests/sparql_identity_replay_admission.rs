//! t_16b96d45 cutover prerequisite: recovery must expose rejected legacy rows.
//! This intentionally RED boundary test pins a storage API gap; it does not
//! implement a migration or change the existing identity/preservation gates.

use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::CellValue;
use ferrosa_sparql::triple_store::partition_key;
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{CommitLog, Mutation, StorageEngine, StorageEngineConfig, TableId};

fn legacy_schema() -> TableSchema {
    let column = |name: &str| ColumnDefinition {
        name: name.into(),
        type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
    };
    TableSchema {
        keyspace: "rdf".into(),
        table: "rdf_triples".into(),
        key_type: "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)".into(),
        clustering_columns: vec![column("predicate"), column("object")],
        static_columns: vec![],
        regular_columns: vec![column("object_type"), column("datatype"), column("language")],
        extensions: Default::default(),
    }
}

#[test]
fn legacy_replay_failure_must_block_cutover_admission() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageEngineConfig::test_config(dir.path());
    let table = TableId::new("rdf", "rdf_triples");
    let key = partition_key("urn:legacy:graph", "urn:legacy:subject");
    let quarantine;
    {
        // Persist the actual old two-component schema BEFORE any SPARQL/v2
        // initialization, exactly as an existing dataset would contain it.
        let storage =
            StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap();
        storage.register_table(legacy_schema()).unwrap();
        storage.flush(&table).unwrap();
        quarantine = storage.table_sstable_dir(&table).join("quarantine");
    }
    // Model a quarantine I/O failure using a deterministic temporary obstacle.
    // Once removed, checking for quarantine JSONL files cannot reveal the loss.
    std::fs::write(&quarantine, b"temporary quarantine I/O obstruction").unwrap();
    let mut clustering = Vec::new();
    for component in ["urn:legacy:predicate", "01"] {
        clustering.extend_from_slice(&(component.len() as u16).to_be_bytes());
        clustering.extend_from_slice(component.as_bytes());
    }
    let survivor = Row {
        clustering,
        cells: vec![
            (0, CellValue::live(b"literal".to_vec(), 1)),
            (
                1,
                CellValue::live(b"http://www.w3.org/2001/XMLSchema#integer".to_vec(), 1),
            ),
        ],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(1),
    };
    let malformed = Row {
        // A truncated u16 length prefix cannot be a legal legacy key.
        clustering: vec![0],
        ..survivor.clone()
    };
    {
        // Bypass current write validation only to seed a historical WAL frame.
        // Both rows are genuinely persisted and reopened through startup replay.
        let log = CommitLog::new(config.commit_log.clone()).unwrap();
        log.append(&Mutation::new(
            "rdf".into(),
            "rdf_triples".into(),
            key.clone(),
            vec![survivor.clone(), malformed],
            1,
        ))
        .unwrap();
        log.force_sync().unwrap();
    }
    let opened = StorageEngine::open(config, None);
    std::fs::remove_file(&quarantine).unwrap();
    match opened {
        Err(error) => {
            let message = error.to_string();
            assert!(
                message.contains("quarantine")
                    || message.contains("malformed")
                    || message.contains("clustering"),
                "startup failed for an unrelated reason: {error}"
            );
            eprintln!("startup correctly refused incomplete legacy replay: {error}");
        }
        Ok((storage, pending)) => {
            assert!(
                pending.is_empty(),
                "registered legacy table should replay at startup"
            );
            let rows = storage.read(&table, &key).unwrap().unwrap().rows;
            assert_eq!(
                rows,
                vec![survivor],
                "the valid original survivor must remain exact"
            );
            let status = storage.replay_set_aside_status();
            eprintln!(
                "startup returned Ok; valid survivors={}; set-aside={status:?}; quarantine artifact exists={}",
                rows.len(),
                quarantine.exists()
            );
            assert!(
                storage.assert_table_recovery_complete(&table).is_err(),
                "cutover cannot certify a complete legacy scan after malformed-row replay/quarantine failure"
            );
        }
    }
}
