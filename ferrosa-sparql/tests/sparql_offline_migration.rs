//! Offline cutover regressions. Corrupt reader frames must block publication.
use ferrosa_common::CellValue;
use ferrosa_sparql::triple_store::{partition_key, rdf_triples_v1_schema};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{CommitLog, Mutation, StorageEngine, StorageEngineConfig, TableId};

const KS: &str = "rdf";
const INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
fn source() -> TableId {
    TableId::new(KS, "rdf_triples")
}
fn legacy_row(value: &str, kind: &str, datatype: Option<&str>, language: Option<&str>) -> Row {
    let mut clustering = Vec::new();
    for component in ["urn:p", value] {
        clustering.extend_from_slice(&(component.len() as u16).to_be_bytes());
        clustering.extend_from_slice(component.as_bytes());
    }
    let mut cells = vec![(0, CellValue::live(kind.as_bytes().to_vec(), 1))];
    for (id, value) in [(1, datatype), (2, language)] {
        if let Some(value) = value {
            cells.push((id, CellValue::live(value.as_bytes().to_vec(), 1)));
        }
    }
    Row {
        clustering,
        cells,
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(1),
    }
}
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb88320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}
fn corrupt_wal(dir: &std::path::Path, valid_crc: bool) {
    let config = StorageEngineConfig::test_config(dir);
    {
        let engine = StorageEngine::new(StorageEngineConfig::test_config(dir), None).unwrap();
        engine.register_table(rdf_triples_v1_schema(KS)).unwrap();
        engine.flush(&source()).unwrap();
    }
    let pos = {
        let log = CommitLog::new(config.commit_log.clone()).unwrap();
        let mutation = Mutation::new(
            KS.into(),
            "rdf_triples".into(),
            partition_key("urn:g", "urn:s"),
            vec![legacy_row("01", "literal", Some(INTEGER), None)],
            1,
        );
        log.append(&mutation).unwrap();
        let pos = log.append(&mutation).unwrap();
        log.shutdown().unwrap();
        pos
    };
    let path = config
        .commit_log
        .log_dir
        .join(format!("commitlog-{}.log", pos.segment_id));
    let mut bytes = std::fs::read(&path).unwrap();
    let offset = pos.offset as usize;
    let len = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    let payload = &mut bytes[offset + 8..offset + 8 + len];
    payload.fill(0xff);
    assert!(Mutation::deserialize_from(payload).is_err());
    let crc = crc32(payload) ^ u32::from(!valid_crc);
    bytes[offset + 8 + len..offset + 12 + len].copy_from_slice(&crc.to_be_bytes());
    std::fs::write(&path, bytes).unwrap();
    std::fs::File::open(&path).unwrap().sync_all().unwrap();
}
async fn assert_rejected_frame_blocks_admission(valid_crc: bool) {
    let dir = tempfile::tempdir().unwrap();
    corrupt_wal(dir.path(), valid_crc);
    use ferrosa_sparql::migration::{
        DeploymentMode, MigrationOptions, OfflineMigration, QuiescedCutover,
    };
    match OfflineMigration::open(
        StorageEngineConfig::test_config(dir.path()),
        DeploymentMode::Direct,
        QuiescedCutover::AllOldReadersWritersAndLifecycleOperationsExcluded,
    ) {
        Err(error) => assert!(
            error.to_string().contains("recovery")
                || error.to_string().contains("mutation")
                || error.to_string().contains("CRC"),
            "unrelated startup error: {error}"
        ),
        Ok(migration) => {
            let refused = migration.run(KS, MigrationOptions::default()).await;
            let marker = migration
                .storage()
                .table_sstable_dir(&source())
                .join("rdf-v2-cutover.json");
            if marker.exists() {
                let value: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(marker).unwrap()).unwrap();
                assert_ne!(
                    value["phase"], "complete",
                    "rejected legacy WAL must never publish completion"
                );
            }
            assert!(
                refused.is_err(),
                "migration must refuse a rejected WAL frame"
            );
            assert!(
                migration
                    .storage()
                    .assert_table_recovery_complete(&source())
                    .is_err(),
                "rejected reader frame must block migration admission (valid_crc={valid_crc})"
            );
        }
    }
}
#[tokio::test]
async fn crc_valid_undecodable_legacy_wal_blocks_migration_admission() {
    assert_rejected_frame_blocks_admission(true).await;
}
#[tokio::test]
async fn crc_invalid_legacy_wal_blocks_migration_admission() {
    assert_rejected_frame_blocks_admission(false).await;
}

#[test]
fn hybrid_legacy_metadata_cannot_be_guessed() {
    let mut row = legacy_row(
        "value",
        "literal",
        Some(ferrosa_sparql::triple_store::RDF_LANG_STRING),
        Some("en"),
    );
    // A newer type/datatype write inherited an older language cell. Even when
    // the resulting metadata looks syntactically valid it is not one term.
    row.cells[2].1.timestamp = 0;
    assert!(ferrosa_sparql::triple_store::decode_v1_row(&row).is_err());
}

#[test]
fn unknown_legacy_metadata_is_not_silently_discarded() {
    let mut row = legacy_row("value", "literal", None, None);
    row.cells
        .push((3, CellValue::live(b"unknown identity".to_vec(), 1)));
    assert!(ferrosa_sparql::triple_store::decode_v1_row(&row).is_err());
}
