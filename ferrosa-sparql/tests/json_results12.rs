//! SPARQL 1.2 JSON Results, pinned to the W3C 2026-08-13 draft.
//! Correctness: preserve directional and recursive RDF terms on the wire.
//! Last revised: 2026-10-27
//! Last changed: Capture missing 1.2 terms through real SELECT expressions.

use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_sparql::engine::{SparqlConfig, SparqlEngine};
use ferrosa_storage::{StorageEngine, StorageEngineConfig};

fn fixture() -> (SparqlEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Arc::new(StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap());
    let path = Arc::new(WritePath::direct(Arc::clone(&storage)));
    (
        SparqlEngine::new(storage, path, SparqlConfig::default()),
        dir,
    )
}

async fn query(engine: &SparqlEngine, text: &str) -> serde_json::Value {
    let result = engine.execute(text, "default").await.unwrap();
    serde_json::from_slice(&result.to_json().unwrap()).unwrap()
}

#[tokio::test]
async fn legacy_scalar_result_keeps_its_existing_shape() {
    let (engine, _dir) = fixture();
    let data = query(&engine, "SELECT (\"plain\" AS ?v) WHERE {}").await;
    assert_eq!(data["head"]["vars"], serde_json::json!(["v"]));
    assert!(data["head"].get("version").is_none());
    assert_eq!(data["results"]["bindings"][0]["v"]["value"], "plain");
}

#[tokio::test]
async fn directional_literal_preserves_language_direction_and_lexical_value() {
    let (engine, _dir) = fixture();
    let data = query(&engine, "SELECT (\"مرحبا\"@ar--rtl AS ?v) WHERE {}").await;
    assert_eq!(data["head"]["version"], "1.2");
    let value = &data["results"]["bindings"][0]["v"];
    assert_eq!(value["type"], "literal");
    assert_eq!(value["value"], "مرحبا");
    assert_eq!(value["xml:lang"], "ar");
    assert_eq!(value["its:dir"], "rtl");
}

#[tokio::test]
async fn triple_term_constant_is_an_object_not_an_encoded_string() {
    let (engine, _dir) = fixture();
    let data = query(
        &engine,
        "SELECT (<<( <urn:s> <urn:p> \"x\" )>> AS ?t) WHERE {}",
    )
    .await;
    assert_eq!(data["head"]["version"], "1.2");
    let term = &data["results"]["bindings"][0]["t"];
    assert_eq!(term["type"], "triple");
    assert_eq!(
        term["value"]["subject"],
        serde_json::json!({"type":"uri","value":"urn:s"})
    );
    assert_eq!(
        term["value"]["predicate"],
        serde_json::json!({"type":"uri","value":"urn:p"})
    );
    assert_eq!(term["value"]["object"]["value"], "x");
}

#[tokio::test]
async fn nested_triple_object_retains_recursive_term_structure() {
    let (engine, _dir) = fixture();
    let data = query(
        &engine,
        "SELECT (TRIPLE(<urn:s>,<urn:p>,TRIPLE(<urn:a>,<urn:b>,\"x\")) AS ?t) WHERE {}",
    )
    .await;
    let object = &data["results"]["bindings"][0]["t"]["value"]["object"];
    assert_eq!(object["type"], "triple");
    assert_eq!(object["value"]["subject"]["value"], "urn:a");
    assert_eq!(object["value"]["object"]["value"], "x");
}

#[tokio::test]
async fn triple_object_preserves_directional_literal_extensions() {
    let (engine, _dir) = fixture();
    let data = query(
        &engine,
        "SELECT (TRIPLE(<urn:s>,<urn:p>,\"مرحبا\"@ar--rtl) AS ?t) WHERE {}",
    )
    .await;
    let object = &data["results"]["bindings"][0]["t"]["value"]["object"];
    assert_eq!(object["value"], "مرحبا");
    assert_eq!(object["xml:lang"], "ar");
    assert_eq!(object["its:dir"], "rtl");
}

#[tokio::test]
async fn invalid_triple_subject_is_an_unbound_expression_result() {
    let (engine, _dir) = fixture();
    let data = query(
        &engine,
        "SELECT (TRIPLE(\"not a subject\",<urn:p>,1) AS ?t) WHERE {}",
    )
    .await;
    assert_eq!(data["results"]["bindings"], serde_json::json!([{}]));
}

#[tokio::test]
async fn rdf12_query_does_not_force_a_version_on_scalar_only_results() {
    let (engine, _dir) = fixture();
    let data = query(
        &engine,
        "SELECT (COUNT(TRIPLE(<urn:s>,<urn:p>,\"x\")) AS ?c) WHERE {}",
    )
    .await;
    assert_eq!(data["results"]["bindings"][0]["c"]["value"], "1");
    assert!(data["head"].get("version").is_none());
}

#[tokio::test]
async fn rdf12_bound_update_terms_are_rejected_before_any_writes() {
    let (engine, _dir) = fixture();
    let result = engine.execute_update("INSERT { <urn:s> <urn:p> ?t . <urn:s> <urn:other> 1 } WHERE { BIND(TRIPLE(<urn:a>,<urn:b>,1) AS ?t) }", "default").await;
    assert!(
        result.is_err(),
        "a bound triple must not silently skip the unsupported write"
    );
    assert_eq!(
        query(&engine, "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }").await["results"]["bindings"]
            [0]["c"]["value"],
        "0"
    );
}

#[tokio::test]
async fn construct_directional_literals_do_not_silently_lose_direction() {
    let (engine, _dir) = fixture();
    let result = engine
        .execute(
            "CONSTRUCT { <urn:s> <urn:p> \"مرحبا\"@ar--rtl } WHERE {}",
            "default",
        )
        .await;
    assert!(
        result.is_err(),
        "RDF 1.2 graph serialization is not implemented; reject rather than drop direction"
    );
}

#[tokio::test]
async fn construct_triple_constant_is_not_silently_skipped() {
    let (engine, _dir) = fixture();
    assert!(engine
        .execute(
            "CONSTRUCT { <urn:s> <urn:p> <<( <urn:a> <urn:b> 1 )>> } WHERE {}",
            "default"
        )
        .await
        .is_err());
}
