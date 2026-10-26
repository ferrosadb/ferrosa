//! t_16b96d45: RDF term identity through the real storage-backed public engine.
//! Core v2 semantics and explicit offline legacy preservation.
//!
//! Test list: same-lexical datatype/language/kind coexistence; typed BGP join
//! without BIND; exact DELETE DATA and pattern deletion; graph isolation;
//! distinct numeric lexical forms; legacy two-component row compatibility.

use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_common::CellValue;
use ferrosa_sparql::engine::{SparqlConfig, SparqlEngine, SparqlResult};
use ferrosa_sparql::triple_store::{partition_key, triples_table_id};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use tempfile::TempDir;

const KS: &str = "rdf";
const G: &str = "urn:identity:graph";
const G2: &str = "urn:identity:other";
const S: &str = "urn:identity:subject";
const P: &str = "urn:identity:typed";
const JOIN: &str = "urn:identity:join";
const INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
const STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
const LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";

type Term = (String, String, Option<String>, Option<String>);

fn literal(value: &str, datatype: &str, language: Option<&str>) -> Term {
    (
        "literal".into(),
        value.into(),
        Some(datatype.into()),
        language.map(str::to_owned),
    )
}

struct Fixture {
    storage: Arc<StorageEngine>,
    engine: SparqlEngine,
    _dir: TempDir,
}

impl Fixture {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            commit_log: CommitLogConfig {
                segment_size: 4096,
                max_segment_age: std::time::Duration::from_secs(60),
                sync_strategy: SyncStrategyConfig::Batch,
                batch: Default::default(),
                log_dir: dir.path().join("commitlog"),
                checkpoint_dir: dir.path().join("commitlog"),
                archive: None,
            },
            compaction: CompactionConfig::from_env(dir.path().join("compaction")),
            object_store: None,
            local_cache_max_bytes: 1024 * 1024,
            local_disk_free_reserve_bytes: 0,
            flush_threshold_bytes: 1024 * 1024,
            memtable_backpressure_bytes: u64::MAX,
            flush_max_age_secs: 5,
            data_dir: dir.path().to_path_buf(),
            index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
            write_verify: true,
            auth_enabled: false,
            auth_warn: false,
            max_pending_replay_mutations_without_schema: 1024,
            memtable_num_shards: 64,
            cache_hot_window_secs: 900,
        };
        let storage = Arc::new(StorageEngine::new(config, None).unwrap());
        let write_path = Arc::new(WritePath::direct(Arc::clone(&storage)));
        let engine = SparqlEngine::new(Arc::clone(&storage), write_path, SparqlConfig::default());
        Self {
            storage,
            engine,
            _dir: dir,
        }
    }

    async fn update(&self, operation: &str, graph: &str, triples: &str) {
        self.engine
            .execute_update(
                &format!("{operation} {{ GRAPH <{graph}> {{ {triples} }} }}"),
                KS,
            )
            .await
            .expect("public update must succeed");
    }

    async fn insert(&self, graph: &str, object: &str) {
        self.update("INSERT DATA", graph, &format!("<{S}> <{P}> {object}"))
            .await;
    }

    async fn terms(&self, query: &str, variable: &str) -> Vec<Term> {
        let SparqlResult::Select(result) = self.engine.execute(query, KS).await.unwrap() else {
            panic!("expected SELECT results");
        };
        let mut terms: Vec<_> = result
            .results
            .bindings
            .into_iter()
            .map(|row| {
                let b = &row[variable];
                (
                    b.binding_type.clone(),
                    b.value.clone(),
                    b.datatype.clone(),
                    b.lang.clone(),
                )
            })
            .collect();
        terms.sort();
        terms
    }

    async fn objects(&self, graph: &str) -> Vec<Term> {
        self.terms(
            &format!("SELECT ?o WHERE {{ GRAPH <{graph}> {{ <{S}> <{P}> ?o }} }}"),
            "o",
        )
        .await
    }

    fn raw_rows(&self, graph: &str) -> Vec<Row> {
        self.storage
            .read(&triples_table_id(KS), &partition_key(graph, S))
            .expect("read real stored partition")
            .expect("seeded partition exists")
            .rows
    }
}

#[tokio::test]
async fn same_lexical_different_datatypes_coexist_in_either_write_order() {
    let f = Fixture::new();
    let integer = format!("\"1\"^^<{INTEGER}>");
    let string = format!("\"1\"^^<{STRING}>");
    for (graph, first, second) in [(G, &integer, &string), (G2, &string, &integer)] {
        f.insert(graph, first).await;
        let before = f.raw_rows(graph);
        f.insert(graph, second).await;
        let after = f.raw_rows(graph);
        eprintln!("graph={graph} first rows={before:?}; after second term rows={after:?}");
    }
    let mut expected = vec![literal("1", INTEGER, None), literal("1", STRING, None)];
    expected.sort();
    // Collect both orders and disk reads before asserting: a RED in the first
    // graph must not hide evidence about the reverse order or the flush path.
    let memory = (f.objects(G).await, f.objects(G2).await);
    f.storage.flush_all().expect("flush identity rows");
    let disk = (f.objects(G).await, f.objects(G2).await);
    assert_eq!(
        (memory, disk),
        (
            (expected.clone(), expected.clone()),
            (expected.clone(), expected)
        ),
        "datatype is part of RDF term identity, in memory and SSTables"
    );
}

#[tokio::test]
async fn typed_join_without_bind_returns_only_the_shared_integer() {
    let f = Fixture::new();
    f.update(
        "INSERT DATA",
        G,
        &format!("<{S}> <{P}> 1 . <{S}> <{P}> \"1\" . <{S}> <{JOIN}> 1"),
    )
    .await;
    let actual = f
        .terms(
            &format!("SELECT ?o WHERE {{ GRAPH <{G}> {{ <{S}> <{P}> ?o . <{S}> <{JOIN}> ?o }} }}"),
            "o",
        )
        .await;
    assert_eq!(actual, vec![literal("1", INTEGER, None)]);
}

#[tokio::test]
async fn deleting_an_absent_integer_does_not_delete_the_same_lexical_string() {
    let f = Fixture::new();
    f.insert(G, "\"1\"").await;
    f.storage.flush_all().unwrap();
    f.update("DELETE DATA", G, &format!("<{S}> <{P}> 1")).await;
    assert_eq!(f.objects(G).await, vec![literal("1", STRING, None)]);
}

#[tokio::test]
async fn deleting_a_present_integer_preserves_its_string_sibling() {
    let f = Fixture::new();
    f.insert(G, "\"1\"").await;
    f.insert(G, "1").await;
    f.update("DELETE DATA", G, &format!("<{S}> <{P}> 1")).await;
    assert_eq!(f.objects(G).await, vec![literal("1", STRING, None)]);
}

#[tokio::test]
async fn pattern_delete_with_integer_constant_preserves_string() {
    let f = Fixture::new();
    f.insert(G, "\"1\"").await;
    f.update("DELETE WHERE", G, &format!("<{S}> <{P}> 1")).await;
    assert_eq!(f.objects(G).await, vec![literal("1", STRING, None)]);
}

#[tokio::test]
async fn literal_constant_matching_checks_datatype_after_storage_read() {
    let f = Fixture::new();
    f.insert(G, "\"1\"").await;
    let matches = f
        .terms(
            &format!("SELECT ?s WHERE {{ GRAPH <{G}> {{ ?s <{P}> 1 }} }}"),
            "s",
        )
        .await;
    assert!(
        matches.is_empty(),
        "integer pattern matched string: {matches:?}"
    );
}

#[tokio::test]
async fn language_tags_distinguish_same_lexical_literals() {
    let f = Fixture::new();
    f.insert(G, "\"chat\"@en").await;
    f.insert(G, "\"chat\"@fr").await;
    assert_eq!(
        f.objects(G).await,
        vec![
            literal("chat", LANG_STRING, Some("en")),
            literal("chat", LANG_STRING, Some("fr"))
        ]
    );
}

#[tokio::test]
async fn iri_and_literal_with_same_characters_coexist() {
    let f = Fixture::new();
    f.insert(G, "<urn:identity:value>").await;
    f.insert(G, "\"urn:identity:value\"").await;
    assert_eq!(
        f.objects(G).await,
        vec![
            literal("urn:identity:value", STRING, None),
            ("uri".into(), "urn:identity:value".into(), None, None)
        ]
    );
}

#[tokio::test]
async fn numeric_value_equality_does_not_collapse_distinct_lexical_forms() {
    let f = Fixture::new();
    f.insert(G, &format!("\"01\"^^<{INTEGER}>")).await;
    f.insert(G, &format!("\"1\"^^<{INTEGER}>")).await;
    f.update("INSERT DATA", G, &format!("<{S}> <{JOIN}> 1"))
        .await;
    assert_eq!(
        f.objects(G).await,
        vec![literal("01", INTEGER, None), literal("1", INTEGER, None)]
    );
    assert_eq!(
        f.terms(
            &format!("SELECT ?o WHERE {{ GRAPH <{G}> {{ <{S}> <{P}> ?o . <{S}> <{JOIN}> ?o }} }}"),
            "o"
        )
        .await,
        vec![literal("1", INTEGER, None)]
    );
}

#[tokio::test]
async fn deleting_one_graph_does_not_reach_identical_terms_in_other_graphs() {
    let f = Fixture::new();
    f.insert(G, "1").await;
    f.insert(G2, "1").await;
    f.engine
        .execute_update(&format!("INSERT DATA {{ <{S}> <{P}> 1 }}"), KS)
        .await
        .unwrap();
    f.storage.flush_all().unwrap();
    f.update("DELETE DATA", G, &format!("<{S}> <{P}> 1")).await;
    assert!(f.objects(G).await.is_empty());
    assert_eq!(f.objects(G2).await, vec![literal("1", INTEGER, None)]);
    assert_eq!(
        f.terms(&format!("SELECT ?o WHERE {{ <{S}> <{P}> ?o }}"), "o")
            .await,
        vec![literal("1", INTEGER, None)]
    );
}

#[tokio::test]
async fn legacy_two_component_row_preserves_literal_metadata_after_flush() {
    // Seed the actual v1 table BEFORE SPARQL activation. This preservation
    // assertion is exercised only after explicit offline migration.
    let dir = TempDir::new().unwrap();
    let storage =
        Arc::new(StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap());
    storage
        .register_table(ferrosa_sparql::triple_store::rdf_triples_v1_schema(KS))
        .unwrap();
    let mut clustering = Vec::new();
    for component in [P, "01"] {
        clustering.extend_from_slice(&(component.len() as u16).to_be_bytes());
        clustering.extend_from_slice(component.as_bytes());
    }
    let timestamp = 1;
    storage
        .write(
            &ferrosa_storage::TableId::new(KS, "rdf_triples"),
            &partition_key(G, S),
            Row {
                clustering,
                // Historic ordinals: object_type=0, datatype=1, language=2.
                cells: vec![
                    (0, CellValue::live(b"literal".to_vec(), timestamp)),
                    (1, CellValue::live(INTEGER.as_bytes().to_vec(), timestamp)),
                ],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(timestamp),
            },
            timestamp,
        )
        .unwrap();
    storage.flush_all().unwrap();
    drop(storage);
    let migration = ferrosa_sparql::migration::OfflineMigration::open(
        StorageEngineConfig::test_config(dir.path()),
        ferrosa_sparql::migration::DeploymentMode::Direct,
        ferrosa_sparql::migration::QuiescedCutover::AllOldReadersWritersAndLifecycleOperationsExcluded,
    ).unwrap();
    migration.run(KS, Default::default()).await.unwrap();
    let storage = migration.storage().clone();
    let engine = SparqlEngine::new(
        storage.clone(),
        Arc::new(WritePath::direct(storage.clone())),
        Default::default(),
    );
    let f = Fixture {
        storage,
        engine,
        _dir: dir,
    };
    // The original exact survivor assertion remains intact after durable cutover.
    assert_eq!(f.objects(G).await, vec![literal("01", INTEGER, None)]);
}
