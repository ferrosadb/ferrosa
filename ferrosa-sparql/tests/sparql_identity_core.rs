//! t_16b96d45: RDF term identity through the real storage-backed public engine.
//! Bounded v2 core tests, independent of migration.
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
use ferrosa_storage::{StorageEngine, StorageEngineConfig};
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

fn core_storage_config(dir: &TempDir) -> StorageEngineConfig {
    let mut config = StorageEngineConfig::test_config(dir.path());
    // Boundary terms themselves exceed the tiny default test WAL segment.
    config.commit_log.segment_size = 1024 * 1024;
    config
}

struct Fixture {
    storage: Arc<StorageEngine>,
    engine: SparqlEngine,
    _dir: TempDir,
}

impl Fixture {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let config = core_storage_config(&dir);
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
async fn v2_schema_conventions_idempotence_and_distinct() {
    let f = Fixture::new();
    for term in [
        "\"1\"",
        "\"1\"",
        "\"1\"^^<http://www.w3.org/2001/XMLSchema#string>",
        "1",
        "\"chat\"@EN",
        "\"chat\"@en",
        "\"chat\"@fr",
        "\"bad\"^^<urn:custom>",
        "\"bad\"^^<http://www.w3.org/2001/XMLSchema#integer>",
    ] {
        f.insert(G, term).await;
    }
    let mut expected = vec![
        literal("1", STRING, None),
        literal("1", INTEGER, None),
        literal("chat", LANG_STRING, Some("en")),
        literal("chat", LANG_STRING, Some("fr")),
        literal("bad", "urn:custom", None),
        literal("bad", INTEGER, None),
    ];
    expected.sort();
    assert_eq!(f.objects(G).await, expected);
    assert_eq!(
        f.terms(
            &format!("SELECT DISTINCT ?o WHERE {{ GRAPH <{G}> {{ <{S}> <{P}> ?o }} }}"),
            "o"
        )
        .await,
        expected
    );
    let schema = f.storage.table_schema(&triples_table_id(KS)).unwrap();
    assert_eq!(schema.table, "rdf_triples_v2");
    assert_eq!(
        schema
            .clustering_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["predicate", "object", "object_identity"]
    );
    assert_eq!(f.raw_rows(G).len(), 6);
}

#[tokio::test]
async fn overflow_in_later_quad_is_rejected_before_mutation() {
    for position in 0..5 {
        let f = Fixture::new();
        let huge = "a".repeat(65536);
        let (g, s, p, o) = match position {
            0 => (format!("urn:{huge}"), S.into(), P.into(), "1".into()),
            1 => (G.into(), format!("urn:{huge}"), P.into(), "1".into()),
            2 => (G.into(), S.into(), format!("urn:{huge}"), "1".into()),
            3 => (G.into(), S.into(), P.into(), format!("\"{huge}\"")),
            _ => (G.into(), S.into(), P.into(), format!("\"x\"^^<urn:{huge}>")),
        };
        let q = format!(
            "INSERT DATA {{ GRAPH <{G}> {{ <{S}> <{P}> 7 }} GRAPH <{g}> {{ <{s}> <{p}> {o} }} }}"
        );
        assert!(
            f.engine.execute_update(&q, KS).await.is_err(),
            "position {position}"
        );
        assert!(
            f.objects(G).await.is_empty(),
            "partial mutation at position {position}"
        );
    }
}

#[tokio::test]
async fn delete_variants_keep_full_bound_identity_and_graphs() {
    for operation in ["DELETE WHERE", "DELETE_INSERT", "CLEAR", "DROP"] {
        let f = Fixture::new();
        f.update(
            "INSERT DATA",
            G,
            &format!("<{S}> <{P}> 1, \"1\", \"1\"@en, \"1\"@fr . <{S}> <{JOIN}> 1"),
        )
        .await;
        f.insert(G2, "1").await;
        let q = match operation {
            "DELETE WHERE" => {
                format!("DELETE WHERE {{ GRAPH <{G}> {{ <{S}> <{P}> ?o . <{S}> <{JOIN}> ?o }} }}")
            }
            "DELETE_INSERT" => format!(
                "DELETE {{ GRAPH <{G}> {{ <{S}> <{P}> ?o }} }} INSERT {{ GRAPH <{G}> {{ <{S}> <urn:moved> ?o }} }} WHERE {{ GRAPH <{G}> {{ <{S}> <{P}> ?o . <{S}> <{JOIN}> ?o }} }}"
            ),
            _ => format!("{operation} GRAPH <{G}>"),
        };
        f.engine.execute_update(&q, KS).await.unwrap();
        let expected = if operation == "CLEAR" || operation == "DROP" {
            vec![]
        } else {
            vec![
                literal("1", LANG_STRING, Some("en")),
                literal("1", LANG_STRING, Some("fr")),
                literal("1", STRING, None),
            ]
        };
        assert_eq!(f.objects(G).await, expected, "{operation}");
        assert_eq!(f.objects(G2).await, vec![literal("1", INTEGER, None)]);
        if operation == "DELETE_INSERT" {
            assert_eq!(
                f.terms(
                    &format!("SELECT ?o WHERE {{ GRAPH <{G}> {{ <{S}> <urn:moved> ?o }} }}"),
                    "o"
                )
                .await,
                vec![literal("1", INTEGER, None)]
            );
        }
        f.insert(G, "1").await;
        assert!(f.objects(G).await.contains(&literal("1", INTEGER, None)));
    }
}

#[tokio::test]
async fn index_and_fallback_postfilter_typed_constants() {
    let f = Fixture::new();
    f.update(
        "INSERT DATA",
        G,
        &format!("<{S}> <{P}> 1, \"1\" . <urn:string-only> <{P}> \"1\""),
    )
    .await;
    let query = format!("SELECT ?s WHERE {{ GRAPH <{G}> {{ ?s ?p 1 }} }}");
    let expected = vec![("uri".into(), S.into(), None, None)];
    for disk in [false, true] {
        if disk {
            f.storage.flush_all().unwrap();
        }
        assert_eq!(f.terms(&query, "s").await, expected);
        f.storage
            .drop_index(&triples_table_id(KS), "rdf_triples_object_idx")
            .unwrap();
        let parsed = spargebra::SparqlParser::new().parse_query(&query).unwrap();
        let plan = ferrosa_sparql::planner::plan_query(
            &parsed,
            &ferrosa_sparql::planner::TripleScope::isolated(KS, G),
        )
        .unwrap();
        let wp = Arc::new(WritePath::direct(f.storage.clone()));
        let out = ferrosa_sparql::executor::execute(&plan, &wp, &Default::default())
            .await
            .unwrap();
        assert_eq!(out.results.bindings.len(), 1);
        assert_eq!(out.results.bindings[0]["s"].value, S);
    }
}

#[tokio::test]
async fn flush_reopen_and_wal_replay_preserve_all_terms_and_exact_delete() {
    for flush in [false, true] {
        let f = Fixture::new();
        f.insert(G, "1").await;
        // Persist schema before the following unflushed writes (WAL branch).
        f.storage.flush_all().unwrap();
        for term in [
            "\"1\"",
            "\"1\"@en",
            "\"1\"@fr",
            "\"01\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        ] {
            f.insert(G, term).await;
        }
        let expected = f.objects(G).await;
        assert_eq!(expected.len(), 5);
        if flush {
            f.storage.flush_all().unwrap();
        }
        let Fixture {
            storage,
            engine,
            _dir: dir,
        } = f;
        drop(engine);
        drop(storage);
        let (storage, pending) = StorageEngine::open(core_storage_config(&dir), None).unwrap();
        assert!(pending.is_empty());
        let storage = Arc::new(storage);
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
        assert_eq!(f.objects(G).await, expected);
        f.update("DELETE DATA", G, &format!("<{S}> <{P}> 1")).await;
        assert_eq!(
            f.objects(G).await,
            expected
                .into_iter()
                .filter(|t| *t != literal("1", INTEGER, None))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            f.terms(
                &format!("SELECT ?s WHERE {{ GRAPH <{G}> {{ ?s ?p \"1\"@fr }} }}"),
                "s"
            )
            .await,
            vec![("uri".into(), S.into(), None, None)]
        );
    }
}

#[tokio::test]
async fn property_paths_preserve_literal_endpoints() {
    let f = Fixture::new();
    f.insert(G, "1").await;
    f.insert(G, "\"1\"").await;
    assert_eq!(
        f.terms(
            &format!("SELECT ?o WHERE {{ GRAPH <{G}> {{ <{S}> <{P}>+ ?o }} }}"),
            "o"
        )
        .await,
        vec![literal("1", INTEGER, None), literal("1", STRING, None)]
    );
}

fn legacy_schema() -> ferrosa_common::schema::TableSchema {
    ferrosa_sparql::triple_store::rdf_triples_v1_schema(KS)
}

#[tokio::test]
async fn legacy_dataset_refuses_reads_and_writes_before_v2_registration() {
    let dir = TempDir::new().unwrap();
    let storage =
        Arc::new(StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap());
    storage.register_table(legacy_schema()).unwrap();
    let table = ferrosa_storage::TableId::new(KS, "rdf_triples");
    let mut clustering = vec![];
    for value in [P, "01"] {
        clustering.extend_from_slice(&(value.len() as u16).to_be_bytes());
        clustering.extend_from_slice(value.as_bytes());
    }
    storage
        .write(
            &table,
            &partition_key(G, S),
            Row {
                clustering,
                cells: vec![
                    (0, CellValue::live(b"literal".to_vec(), 1)),
                    (1, CellValue::live(INTEGER.as_bytes().to_vec(), 1)),
                ],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1),
            },
            1,
        )
        .unwrap();
    storage.flush_all().unwrap();
    let engine = SparqlEngine::new(
        storage.clone(),
        Arc::new(WritePath::direct(storage.clone())),
        SparqlConfig {
            default_graph: KS.into(),
            ..Default::default()
        },
    );
    for result in [
        engine
            .execute("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", KS)
            .await
            .map(|_| ()),
        engine
            .execute_update("INSERT DATA { <urn:s> <urn:p> 1 }", KS)
            .await
            .map(|_| ()),
    ] {
        let error = result
            .expect_err("legacy data must not silently vanish behind v2")
            .to_string();
        assert!(
            error.contains("rdf_triples") && error.contains("migration"),
            "{error}"
        );
    }
    assert!(storage
        .table_schema(&ferrosa_storage::TableId::new(KS, "rdf_triples_v2"))
        .is_none());
    assert_eq!(
        storage
            .read(&table, &partition_key(G, S))
            .unwrap()
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[tokio::test]
async fn malformed_identity_or_key_cell_mismatch_is_a_query_error() {
    for corruption in ["version", "cell", "partition"] {
        let f = Fixture::new();
        f.insert(G, "1").await;
        let mut row = f.raw_rows(G).pop().unwrap();
        let mut key = partition_key(G, S);
        match corruption {
            "version" => {
                // third component starts after the predicate and lexical value.
                let at = 2 + P.len() + 2 + 1 + 2;
                if row.clustering.len() > at {
                    row.clustering[at] = 99;
                } else {
                    row.clustering.extend_from_slice(&[0, 1, 99]);
                }
            }
            "cell" => row.cells[0].1 = CellValue::live(b"uri".to_vec(), i64::MAX - 1),
            _ => {
                key = ferrosa_common::key::DecoratedKey::new(
                    ferrosa_common::key::PartitionKey::new(vec![0, 1, b'x']),
                )
            }
        }
        row.primary_key_liveness = LivenessInfo::with_timestamp(i64::MAX - 1);
        // Shape validation may reject a corrupted key before it reaches reads.
        match f
            .storage
            .write(&triples_table_id(KS), &key, row, i64::MAX - 1)
        {
            Err(_) if corruption == "version" => (),
            r => {
                r.unwrap();
                assert!(
                    f.engine
                        .execute(
                            &format!("SELECT ?s ?p ?o WHERE {{ GRAPH <{G}> {{ ?s ?p ?o }} }}"),
                            KS
                        )
                        .await
                        .is_err(),
                    "{corruption}"
                );
            }
        }
    }
}

#[tokio::test]
async fn same_term_filter_preserves_numeric_lexical_and_language_identity() {
    let f = Fixture::new();
    for term in [
        "1",
        "\"01\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        "\"1\"@en",
        "\"1\"@fr",
        "\"1\"^^<urn:custom>",
    ] {
        f.insert(G, term).await;
    }
    for (constant, expected) in [
        ("1", literal("1", INTEGER, None)),
        ("\"1\"@EN", literal("1", LANG_STRING, Some("en"))),
        ("\"1\"^^<urn:custom>", literal("1", "urn:custom", None)),
    ] {
        assert_eq!(f.terms(&format!("SELECT ?o WHERE {{ GRAPH <{G}> {{ <{S}> <{P}> ?o FILTER(sameTerm(?o,{constant})) }} }}"),"o").await,vec![expected]);
    }
}

#[tokio::test]
async fn clear_drop_target_variants_keep_the_other_graphs() {
    for verb in ["CLEAR", "DROP"] {
        for target in ["DEFAULT", "NAMED", "ALL"] {
            let f = Fixture::new();
            f.engine
                .execute_update(&format!("INSERT DATA {{ <{S}> <{P}> 1, \"1\" }}"), KS)
                .await
                .unwrap();
            for g in [G, G2] {
                f.insert(g, "1").await;
                f.insert(g, "\"1\"").await;
            }
            f.engine
                .execute_update(&format!("{verb} {target}"), KS)
                .await
                .unwrap();
            let default = f
                .terms(&format!("SELECT ?o WHERE {{ <{S}> <{P}> ?o }}"), "o")
                .await;
            let both = vec![literal("1", INTEGER, None), literal("1", STRING, None)];
            assert_eq!(
                default,
                if target == "NAMED" {
                    both.clone()
                } else {
                    vec![]
                },
                "{verb} {target}"
            );
            for g in [G, G2] {
                assert_eq!(
                    f.objects(g).await,
                    if target == "DEFAULT" {
                        both.clone()
                    } else {
                        vec![]
                    },
                    "{verb} {target}"
                );
            }
        }
    }
}

#[tokio::test]
async fn maximum_lexical_component_and_delimiter_text_survive_flush() {
    let f = Fixture::new();
    let lexical = "é".repeat(32767) + "x";
    let quoted = serde_json::to_string(&lexical).unwrap();
    f.insert(G, &quoted).await;
    f.insert(G, "\"a\\u0000|:b\"^^<urn:custom>").await;
    f.storage.flush_all().unwrap();
    let expected = vec![
        literal("a\0|:b", "urn:custom", None),
        literal(&lexical, STRING, None),
    ];
    assert_eq!(f.objects(G).await, expected);
    f.update("DELETE DATA", G, &format!("<{S}> <{P}> {quoted}"))
        .await;
    assert_eq!(
        f.objects(G).await,
        vec![literal("a\0|:b", "urn:custom", None)]
    );
}

#[test]
fn v1_decoder_preserves_historical_ordinals_and_literal_conventions() {
    use ferrosa_sparql::triple_store::{decode_v1_row, rdf_triples_schema, rdf_triples_v1_schema};
    assert!(rdf_triples_schema(KS)
        .legacy_storage_column_order_warning()
        .is_none());
    assert!(rdf_triples_v1_schema(KS)
        .legacy_storage_column_order_warning()
        .is_some());
    let mut key = vec![];
    for c in [P, "01"] {
        key.extend_from_slice(&(c.len() as u16).to_be_bytes());
        key.extend_from_slice(c.as_bytes());
    }
    for (datatype, lang, expected) in [
        (Some(INTEGER), None, literal("01", INTEGER, None)),
        (None, None, literal("01", STRING, None)),
        (None, Some("EN"), literal("01", LANG_STRING, Some("en"))),
    ] {
        let mut row = Row {
            clustering: key.clone(),
            cells: vec![(0, CellValue::live(b"literal".to_vec(), 1))],
            deletion: DeletionTime::LIVE,
            primary_key_liveness: LivenessInfo::with_timestamp(1),
        };
        if let Some(dt) = datatype {
            row.cells
                .push((1, CellValue::live(dt.as_bytes().to_vec(), 1)));
        }
        if let Some(lang) = lang {
            row.cells
                .push((2, CellValue::live(lang.as_bytes().to_vec(), 1)));
        }
        let (p, t) = decode_v1_row(&row).unwrap().unwrap();
        assert_eq!(p, P);
        assert_eq!((t.binding_type, t.value, t.datatype, t.lang), expected);
        row.clustering.push(0);
        assert!(decode_v1_row(&row).is_err());
    }
}

#[tokio::test]
async fn insert_where_and_language_deletes_keep_full_terms() {
    let f = Fixture::new();
    for term in [
        "\"01\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        "\"01\"",
        "\"01\"@EN",
        "\"01\"@fr",
        "\"01\"^^<urn:custom>",
    ] {
        f.insert(G, term).await;
    }
    let expected = f.objects(G).await;
    f.engine.execute_update(&format!("INSERT {{ GRAPH <{G}> {{ <{S}> <urn:copy> ?copy }} }} WHERE {{ GRAPH <{G}> {{ <{S}> <{P}> ?o BIND(?o AS ?copy) }} }}"),KS).await.unwrap();
    assert_eq!(
        f.terms(
            &format!("SELECT ?o WHERE {{ GRAPH <{G}> {{ <{S}> <urn:copy> ?o }} }}"),
            "o"
        )
        .await,
        expected
    );
    f.update("DELETE DATA", G, &format!("<{S}> <{P}> \"01\"@en"))
        .await;
    let expected = expected
        .into_iter()
        .filter(|t| *t != literal("01", LANG_STRING, Some("en")))
        .collect::<Vec<_>>();
    assert_eq!(f.objects(G).await, expected);
    f.update(
        "DELETE WHERE",
        G,
        &format!("<{S}> <{P}> \"01\"^^<urn:custom>"),
    )
    .await;
    assert_eq!(
        f.objects(G).await,
        expected
            .into_iter()
            .filter(|t| *t != literal("01", "urn:custom", None))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn oversized_bound_insert_is_rejected_before_its_delete() {
    let f = Fixture::new();
    f.insert(G, "1").await;
    let huge = "x".repeat(65536);
    let q = format!(
        "DELETE {{ GRAPH <{G}> {{ <{S}> <{P}> ?o }} }} INSERT {{ GRAPH <{G}> {{ <{S}> <{P}> ?large }} }} WHERE {{ GRAPH <{G}> {{ <{S}> <{P}> ?o BIND(\"{huge}\" AS ?large) }} }}"
    );
    assert!(f.engine.execute_update(&q, KS).await.is_err());
    assert_eq!(f.objects(G).await, vec![literal("1", INTEGER, None)]);
}

#[tokio::test]
async fn ask_and_construct_use_full_term_matching() {
    let f = Fixture::new();
    f.insert(G, "\"1\"").await;
    let SparqlResult::Ask(result) = f
        .engine
        .execute(&format!("ASK {{ GRAPH <{G}> {{ <{S}> <{P}> 1 }} }}"), KS)
        .await
        .unwrap()
    else {
        panic!("ASK");
    };
    assert!(!result.boolean);
    f.insert(G, "1").await;
    let SparqlResult::Graph(result) = f
        .engine
        .execute(
            &format!("CONSTRUCT {{ <{S}> <{P}> ?o }} WHERE {{ GRAPH <{G}> {{ <{S}> <{P}> ?o }} }}"),
            KS,
        )
        .await
        .unwrap()
    else {
        panic!("CONSTRUCT");
    };
    let mut terms = result
        .into_iter()
        .map(|t| (t.object_type, t.object, t.datatype, t.lang))
        .collect::<Vec<_>>();
    terms.sort();
    assert_eq!(
        terms,
        vec![literal("1", INTEGER, None), literal("1", STRING, None)]
    );
}

#[tokio::test]
async fn clear_wide_graph_preflights_limits_then_deletes_every_fragment() {
    let f = Fixture::new();
    for i in 0..600 {
        f.insert(G, &format!("\"term-{i}\"")).await;
    }
    f.storage.flush(&triples_table_id(KS)).unwrap();
    let bounded = SparqlEngine::new(
        f.storage.clone(),
        Arc::new(WritePath::direct(f.storage.clone())),
        SparqlConfig {
            max_rows: 599,
            ..Default::default()
        },
    );
    assert!(bounded
        .execute_update(&format!("CLEAR GRAPH <{G}>"), KS)
        .await
        .is_err());
    assert_eq!(
        f.objects(G).await.len(),
        600,
        "failed preflight must not delete a prefix"
    );
    f.engine
        .execute_update(&format!("CLEAR GRAPH <{G}>"), KS)
        .await
        .unwrap();
    assert!(
        f.objects(G).await.is_empty(),
        "all fragments must be deleted"
    );
}
