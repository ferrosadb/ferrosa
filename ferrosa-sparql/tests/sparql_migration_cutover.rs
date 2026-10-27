//! Explicit offline migration: durable refusal until publication, retry, and v2-only serving.
use ferrosa_cluster::write_path::WritePath;
use ferrosa_common::CellValue;
use ferrosa_sparql::{
    engine::{SparqlConfig, SparqlEngine, SparqlResult},
    migration::{DeploymentMode, MigrationOptions, OfflineMigration, QuiescedCutover},
    triple_store::*,
};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{StorageEngine, StorageEngineConfig, TableId};
use std::sync::Arc;
const KS: &str = "rdf";
const INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
fn source() -> TableId {
    TableId::new(KS, RDF_TRIPLES_V1_TABLE)
}
fn confirmation() -> QuiescedCutover {
    QuiescedCutover::AllOldReadersWritersAndLifecycleOperationsExcluded
}
fn config(dir: &std::path::Path) -> StorageEngineConfig {
    StorageEngineConfig::test_config(dir)
}
fn row(value: &str, kind: &str, dt: Option<&str>, lang: Option<&str>) -> Row {
    let mut clustering = vec![];
    for s in ["urn:p", value] {
        clustering.extend_from_slice(&(s.len() as u16).to_be_bytes());
        clustering.extend_from_slice(s.as_bytes());
    }
    let mut cells = vec![(0, CellValue::live(kind.as_bytes().to_vec(), 1))];
    for (id, value) in [(1, dt), (2, lang)] {
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
fn seed(dir: &std::path::Path, rows: Vec<Row>, flush: bool) {
    let engine = StorageEngine::new(config(dir), None).unwrap();
    engine.register_table(rdf_triples_v1_schema(KS)).unwrap();
    engine.flush(&source()).unwrap();
    for row in rows {
        engine
            .write(&source(), &partition_key("urn:g", "urn:s"), row, 1)
            .unwrap();
    }
    if flush {
        engine.flush(&source()).unwrap();
    }
}
fn open(dir: &std::path::Path) -> OfflineMigration {
    OfflineMigration::open(config(dir), DeploymentMode::Direct, confirmation()).unwrap()
}
fn serving(storage: Arc<StorageEngine>) -> SparqlEngine {
    SparqlEngine::new(
        storage.clone(),
        Arc::new(WritePath::direct(storage)),
        SparqlConfig::default(),
    )
}
async fn terms(
    engine: &SparqlEngine,
) -> Result<Vec<ferrosa_sparql::results::Binding>, ferrosa_sparql::error::SparqlError> {
    let SparqlResult::Select(result) = engine
        .execute(
            "SELECT ?o WHERE { GRAPH <urn:g> { <urn:s> <urn:p> ?o } }",
            KS,
        )
        .await?
    else {
        panic!("expected SELECT")
    };
    Ok(result
        .results
        .bindings
        .into_iter()
        .map(|mut row| row.remove("o").unwrap())
        .collect())
}
#[tokio::test]
async fn migrate_replayed_survivors_with_conventions_and_no_v1_resurrection() {
    let dir = tempfile::tempdir().unwrap();
    seed(
        dir.path(),
        vec![
            row("01", "literal", Some(INTEGER), None),
            row("plain", "literal", None, None),
            row("bonjour", "literal", None, Some("FR")),
            row("urn:object", "uri", None, None),
            row("blank", "bnode", None, None),
        ],
        false,
    );
    {
        let migration = open(dir.path());
        assert!(terms(&serving(migration.storage().clone())).await.is_err());
        let report = migration
            .run(KS, MigrationOptions::default())
            .await
            .unwrap();
        assert_eq!(report.survivors, 5);
    }
    let (storage, pending) = StorageEngine::open(config(dir.path()), None).unwrap();
    storage.replay_mutations(pending).unwrap();
    let storage = Arc::new(storage);
    let engine = serving(storage.clone());
    let got = terms(&engine).await.unwrap();
    assert_eq!(got.len(), 5);
    assert!(got
        .iter()
        .any(|b| b.scalar_value().unwrap() == "01" && b.datatype() == Some(INTEGER)));
    assert!(got
        .iter()
        .any(|b| b.scalar_value().unwrap() == "plain" && b.datatype() == Some(XSD_STRING)));
    assert!(got.iter().any(|b| b.lang() == Some("fr")));
    assert!(got.iter().any(|b| b.kind() == "uri"));
    assert!(got.iter().any(|b| b.kind() == "bnode"));
    engine.execute_update("DELETE DATA { GRAPH <urn:g> { <urn:s> <urn:p> \"01\"^^<http://www.w3.org/2001/XMLSchema#integer> } }", KS).await.unwrap();
    storage.flush(&triples_table_id(KS)).unwrap();
    drop(engine);
    drop(storage);
    let migration = open(dir.path());
    // A completed run must not recopy the retired source after a v2 delete.
    migration
        .run(KS, MigrationOptions::default())
        .await
        .unwrap();
    let engine = serving(migration.storage().clone());
    assert_eq!(terms(&engine).await.unwrap().len(), 4);
    engine.execute_update("INSERT DATA { GRAPH <urn:g> { <urn:s> <urn:p> \"01\"^^<http://www.w3.org/2001/XMLSchema#integer> } }", KS).await.unwrap();
    assert_eq!(terms(&engine).await.unwrap().len(), 5);
    assert!(migration.storage().table_schema(&source()).is_some());
}
#[tokio::test]
async fn interrupted_copy_is_unservable_and_resumes_without_reset() {
    let dir = tempfile::tempdir().unwrap();
    seed(
        dir.path(),
        (0..12)
            .map(|i| row(&format!("v{i}"), "literal", None, None))
            .collect(),
        true,
    );
    {
        let migration = open(dir.path());
        let error = migration
            .run(
                KS,
                MigrationOptions {
                    max_rows_per_run: Some(3),
                },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("budget"), "{error}");
        assert!(terms(&serving(migration.storage().clone())).await.is_err());
        assert!(migration
            .storage()
            .read(&triples_table_id(KS), &partition_key("urn:g", "urn:s"))
            .unwrap()
            .is_some());
    }
    let migration = open(dir.path());
    assert!(terms(&serving(migration.storage().clone())).await.is_err());
    migration
        .run(KS, MigrationOptions::default())
        .await
        .unwrap();
    assert_eq!(
        terms(&serving(migration.storage().clone()))
            .await
            .unwrap()
            .len(),
        12
    );
}
#[tokio::test]
async fn malformed_metadata_never_publishes_and_partial_target_stays_fenced() {
    let dir = tempfile::tempdir().unwrap();
    seed(
        dir.path(),
        vec![row("bad", "literal", Some(INTEGER), Some("en"))],
        true,
    );
    let migration = open(dir.path());
    assert!(migration
        .run(KS, MigrationOptions::default())
        .await
        .is_err());
    assert!(terms(&serving(migration.storage().clone())).await.is_err());
}
#[test]
fn non_direct_modes_refuse_before_creating_storage() {
    for mode in [
        DeploymentMode::Pair,
        DeploymentMode::Cluster,
        DeploymentMode::Degraded,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent");
        assert!(OfflineMigration::open(config(&path), mode, confirmation()).is_err());
        assert!(
            !path.exists(),
            "mode refusal must precede any storage mutation"
        );
    }
}
#[tokio::test]
async fn fresh_populated_v2_is_never_claimed_as_partial_migration() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), vec![row("old", "literal", None, None)], true);
    {
        let (storage, pending) = StorageEngine::open(config(dir.path()), None).unwrap();
        storage.replay_mutations(pending).unwrap();
        storage.register_table(rdf_triples_schema(KS)).unwrap();
        let term = canonical_term("literal", "fresh", None, None).unwrap();
        storage
            .write(
                &triples_table_id(KS),
                &partition_key("urn:g", "urn:s"),
                Row {
                    clustering: encode_clustering("urn:p", &term).unwrap(),
                    cells: vec![
                        (
                            COL_DATATYPE,
                            CellValue::live(XSD_STRING.as_bytes().to_vec(), 2),
                        ),
                        (COL_OBJECT_TYPE, CellValue::live(b"literal".to_vec(), 2)),
                    ],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(2),
                },
                2,
            )
            .unwrap();
        storage.flush(&triples_table_id(KS)).unwrap();
    }
    let migration = open(dir.path());
    assert!(migration
        .run(KS, MigrationOptions::default())
        .await
        .unwrap_err()
        .to_string()
        .contains("target"));
    let target = migration
        .storage()
        .read(&triples_table_id(KS), &partition_key("urn:g", "urn:s"))
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_v2_row(&target.rows[0])
            .unwrap()
            .unwrap()
            .1
            .scalar_value()
            .unwrap(),
        "fresh"
    );
}
#[tokio::test]
async fn empty_source_requires_recovery_and_has_durable_cutover() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), vec![], true);
    let migration = open(dir.path());
    assert_eq!(
        migration
            .run(KS, MigrationOptions::default())
            .await
            .unwrap()
            .survivors,
        0
    );
    assert!(terms(&serving(migration.storage().clone()))
        .await
        .unwrap()
        .is_empty());
}
#[tokio::test]
async fn replaced_source_incarnation_invalidates_completion() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), vec![row("old", "literal", None, None)], true);
    let migration = open(dir.path());
    migration
        .run(KS, MigrationOptions::default())
        .await
        .unwrap();
    migration.storage().unregister_table(&source()).unwrap();
    migration
        .storage()
        .register_table(rdf_triples_v1_schema(KS))
        .unwrap();
    assert!(terms(&serving(migration.storage().clone())).await.is_err());
    assert!(migration
        .run(KS, MigrationOptions::default())
        .await
        .is_err());
}
#[test]
fn migration_must_not_materialize_whole_tables_or_index_results() {
    let source = include_str!("../src/migration.rs");
    for forbidden in [
        ".range_iter(",
        ".read_range(",
        ".read_by_index_in_partition(",
        ".collect::<Vec",
        ".collect::<std::collections",
    ] {
        assert!(
            !source.contains(forbidden),
            "unbounded migration path: {forbidden}"
        );
    }
    assert!(source.contains("range_iter_fragmented"));
}

#[tokio::test]
async fn wide_source_streams_beyond_a_fragment_and_preserves_literal_variants() {
    let dir = tempfile::tempdir().unwrap();
    let mut rows = (0..600)
        .map(|i| row(&format!("v{i}"), "literal", Some(INTEGER), None))
        .collect::<Vec<_>>();
    rows.extend([
        row("01", "literal", Some(INTEGER), None),
        row("1", "literal", Some(INTEGER), None),
        row("custom", "literal", Some("urn:custom"), None),
        row("not-a-number", "literal", Some(INTEGER), None),
    ]);
    seed(dir.path(), rows, true);
    let migration = open(dir.path());
    assert_eq!(
        migration
            .run(KS, MigrationOptions::default())
            .await
            .unwrap()
            .survivors,
        604
    );
    let got = terms(&serving(migration.storage().clone())).await.unwrap();
    assert_eq!(got.len(), 604);
    assert!(got
        .iter()
        .any(|b| b.scalar_value().unwrap() == "custom" && b.datatype() == Some("urn:custom")));
    assert!(got
        .iter()
        .any(|b| b.scalar_value().unwrap() == "not-a-number" && b.datatype() == Some(INTEGER)));
}

#[tokio::test]
async fn copied_source_changed_during_interruption_refuses_retry() {
    let dir = tempfile::tempdir().unwrap();
    seed(
        dir.path(),
        vec![
            row("one", "literal", None, None),
            row("two", "literal", None, None),
        ],
        true,
    );
    let migration = open(dir.path());
    assert!(migration
        .run(
            KS,
            MigrationOptions {
                max_rows_per_run: Some(1)
            }
        )
        .await
        .is_err());
    migration.storage().truncate(&source()).unwrap();
    assert!(migration
        .run(KS, MigrationOptions::default())
        .await
        .is_err());
    assert!(terms(&serving(migration.storage().clone())).await.is_err());
}

#[tokio::test]
async fn empty_registry_keeps_flat_source_and_target_visible_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    seed(
        dir.path(),
        vec![row("01", "literal", Some(INTEGER), None)],
        true,
    );
    ferrosa_storage::schema_snapshot::SchemaSnapshotStore::new(dir.path())
        .persist(&ferrosa_schema::SchemaSnapshot::default())
        .unwrap();
    {
        let migration = open(dir.path());
        migration
            .run(KS, MigrationOptions::default())
            .await
            .unwrap();
    }
    let migration = open(dir.path());
    assert_eq!(
        migration
            .run(KS, MigrationOptions::default())
            .await
            .unwrap()
            .survivors,
        1
    );
    assert_eq!(
        terms(&serving(migration.storage().clone())).await.unwrap()[0]
            .scalar_value()
            .unwrap(),
        "01"
    );
}

#[tokio::test]
async fn failed_publication_and_malformed_marker_never_admit_serving() {
    let dir = tempfile::tempdir().unwrap();
    seed(
        dir.path(),
        vec![
            row("one", "literal", None, None),
            row("two", "literal", None, None),
        ],
        true,
    );
    let migration = open(dir.path());
    assert!(migration
        .run(
            KS,
            MigrationOptions {
                max_rows_per_run: Some(1)
            }
        )
        .await
        .is_err());
    let marker = migration
        .storage()
        .table_sstable_dir(&triples_table_id(KS))
        .join("rdf-v2-cutover.json");
    let incomplete = std::fs::read(&marker).unwrap();
    std::fs::remove_file(&marker).unwrap();
    std::fs::create_dir(&marker).unwrap();
    assert!(migration
        .run(KS, MigrationOptions::default())
        .await
        .is_err());
    assert!(terms(&serving(migration.storage().clone())).await.is_err());
    std::fs::remove_dir(&marker).unwrap();
    std::fs::write(&marker, b"{broken").unwrap();
    assert!(terms(&serving(migration.storage().clone())).await.is_err());
    std::fs::write(&marker, incomplete).unwrap();
    migration
        .run(KS, MigrationOptions::default())
        .await
        .unwrap();
    assert_eq!(
        terms(&serving(migration.storage().clone()))
            .await
            .unwrap()
            .len(),
        2
    );
}
