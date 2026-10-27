//! SPARQL COUNT semantics over the real typed RDF store.
//! Correctness: global/grouped counts, empty input, term identity and scan bounds.
//! Last revised: 2026-10-27
//! Last changed: Pin the live COUNT rejection before adding aggregation.

use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_sparql::engine::{SparqlConfig, SparqlEngine, SparqlResult};
use ferrosa_storage::{StorageEngine, StorageEngineConfig};

const KS: &str = "default";

fn fixture(max_rows: usize) -> (SparqlEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageEngineConfig::test_config(dir.path());
    config.commit_log.segment_size = 1024 * 1024;
    let storage = Arc::new(StorageEngine::new(config, None).unwrap());
    let path = Arc::new(WritePath::direct(Arc::clone(&storage)));
    (
        SparqlEngine::new(
            storage,
            path,
            SparqlConfig {
                default_graph: KS.into(),
                max_rows,
            },
        ),
        dir,
    )
}

async fn seed(engine: &SparqlEngine) {
    engine
        .execute_update(
            "INSERT DATA { <urn:a> <urn:p> 1 . <urn:b> <urn:p> 1 . <urn:c> <urn:p> 2 }",
            KS,
        )
        .await
        .unwrap();
}

async fn count(engine: &SparqlEngine, query: &str) -> u64 {
    let SparqlResult::Select(result) = engine.execute(query, KS).await.unwrap() else {
        panic!("COUNT must return a SELECT result");
    };
    assert_eq!(result.results.bindings.len(), 1);
    let value = &result.results.bindings[0]["c"];
    assert_eq!(value.kind(), "literal");
    assert_eq!(
        value.datatype(),
        Some("http://www.w3.org/2001/XMLSchema#integer")
    );
    value.scalar_value().unwrap().parse().unwrap()
}

#[tokio::test]
async fn count_star_and_bound_variable_preserve_bag_multiplicity() {
    let (engine, _dir) = fixture(64);
    seed(&engine).await;
    for aggregate in ["*", "?o"] {
        let query = format!("SELECT (COUNT({aggregate}) AS ?c) WHERE {{ ?s <urn:p> ?o }}");
        assert_eq!(count(&engine, &query).await, 3);
    }
    assert_eq!(
        count(
            &engine,
            "SELECT (COUNT(?missing) AS ?c) WHERE { ?s <urn:p> ?o }"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn global_count_of_empty_input_is_one_zero_row() {
    let (engine, _dir) = fixture(64);
    assert_eq!(
        count(&engine, "SELECT (COUNT(*) AS ?c) WHERE { ?s <urn:p> ?o }").await,
        0
    );
}

#[tokio::test]
async fn distinct_count_preserves_rdf_term_identity() {
    let (engine, _dir) = fixture(64);
    engine
        .execute_update(
            "INSERT DATA { <urn:a> <urn:p> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> . \
         <urn:b> <urn:p> \"01\"^^<http://www.w3.org/2001/XMLSchema#integer> . \
         <urn:c> <urn:p> \"1\" }",
            KS,
        )
        .await
        .unwrap();
    assert_eq!(
        count(
            &engine,
            "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s <urn:p> ?o }"
        )
        .await,
        3
    );
}

#[tokio::test]
async fn grouped_count_emits_one_row_per_group() {
    let (engine, _dir) = fixture(64);
    seed(&engine).await;
    let SparqlResult::Select(result) = engine
        .execute(
            "SELECT ?o (COUNT(*) AS ?c) WHERE { ?s <urn:p> ?o } GROUP BY ?o ORDER BY ?o",
            KS,
        )
        .await
        .unwrap()
    else {
        panic!("expected SELECT");
    };
    let rows: Vec<_> = result
        .results
        .bindings
        .iter()
        .map(|row| {
            (
                row["o"].scalar_value().unwrap(),
                row["c"].scalar_value().unwrap(),
            )
        })
        .collect();
    assert_eq!(rows, [("1", "2"), ("2", "1")]);
}

#[tokio::test]
async fn count_named_graph_does_not_mix_default_graph_rows() {
    let (engine, _dir) = fixture(64);
    seed(&engine).await;
    engine
        .execute_update("INSERT DATA { GRAPH <urn:g> { <urn:n> <urn:p> 9 } }", KS)
        .await
        .unwrap();
    assert_eq!(
        count(
            &engine,
            "SELECT (COUNT(*) AS ?c) WHERE { GRAPH <urn:g> { ?s <urn:p> ?o } }"
        )
        .await,
        1
    );
    assert_eq!(
        count(&engine, "SELECT (COUNT(*) AS ?c) WHERE { ?s <urn:p> ?o }").await,
        3
    );
}

#[tokio::test]
async fn limit_applies_after_count_and_scan_limits_remain_loud() {
    let (engine, _dir) = fixture(64);
    seed(&engine).await;
    assert_eq!(
        count(
            &engine,
            "SELECT (COUNT(*) AS ?c) WHERE { ?s <urn:p> ?o } LIMIT 1"
        )
        .await,
        3
    );
    let (bounded, _bounded_dir) = fixture(2);
    seed(&bounded).await;
    let error = bounded
        .execute("SELECT (COUNT(*) AS ?c) WHERE { ?s <urn:p> ?o }", KS)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("bound"), "{error}");
}

#[tokio::test]
async fn count_preserves_join_and_filter_multiplicity() {
    let (engine, _dir) = fixture(64);
    seed(&engine).await;
    assert_eq!(
        count(
            &engine,
            "SELECT (COUNT(*) AS ?c) WHERE { ?s <urn:p> ?o . ?t <urn:p> ?o FILTER(?o = 1) }"
        )
        .await,
        4
    );
    assert_eq!(
        count(
            &engine,
            "SELECT (COUNT(DISTINCT *) AS ?c) WHERE { ?s <urn:p> ?o }"
        )
        .await,
        3
    );
}
#[tokio::test]
async fn count_fails_loud_on_unimplemented_input_algebra() {
    let (engine, _dir) = fixture(64);
    seed(&engine).await;
    for query in [
        "SELECT (COUNT(*) AS ?c) WHERE { { ?s <urn:p> ?o } UNION { ?s <urn:p> ?o } }",
        "SELECT (COUNT(*) AS ?c) WHERE { ?s <urn:p> ?o OPTIONAL { ?s <urn:q> ?v } }",
        "SELECT (COUNT(*) AS ?c) WHERE { { SELECT ?s WHERE { ?s <urn:p> ?o } LIMIT 1 } }",
    ] {
        assert!(
            engine.execute(query, KS).await.is_err(),
            "must not publish an incorrect count for {query}"
        );
    }
}
