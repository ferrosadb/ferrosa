//! Integration tests for SPARQL 1.1 pattern-based UPDATE operations.
//!
//! Covers URS-QEC-D04 (T-QEC-D05/D06): DELETE WHERE, DELETE/INSERT … WHERE,
//! and CLEAR GRAPH executed over the `rdf_triples` store, verified through the
//! SELECT executor (a delete is invisible to a subsequent SELECT).

use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_sparql::engine::{SparqlConfig, SparqlEngine, SparqlResult};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use tempfile::TempDir;

// Use "default" as the keyspace so the partition-key graph component written by
// INSERT DATA (DefaultGraph -> "default") matches the graph the SELECT executor
// and the pattern-delete bind against. This exercises a true insert→delete→read
// round-trip without the keyspace/graph coupling getting in the way.
const KS: &str = "default";

fn setup() -> (Arc<StorageEngine>, Arc<WritePath>, TempDir) {
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
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
        data_dir: dir.path().to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        write_verify: true,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
    };
    let storage = Arc::new(StorageEngine::new(config, None).unwrap());
    let write_path = Arc::new(WritePath::direct(Arc::clone(&storage)));
    (storage, write_path, dir)
}

fn engine(storage: Arc<StorageEngine>, write_path: Arc<WritePath>) -> SparqlEngine {
    let config = SparqlConfig {
        default_graph: KS.to_string(),
        ..Default::default()
    };
    SparqlEngine::new(storage, write_path, config)
}

/// Run a SELECT and return the number of result rows.
async fn select_count(eng: &SparqlEngine, query: &str) -> usize {
    match eng.execute(query, KS).await.expect("select must succeed") {
        SparqlResult::Select(r) => r.results.bindings.len(),
        SparqlResult::Ask(_) => panic!("expected SELECT result, got ASK"),
    }
}

/// Run a SELECT and collect the bound values of `var` across all result rows.
async fn select_values(eng: &SparqlEngine, query: &str, var: &str) -> Vec<String> {
    match eng.execute(query, KS).await.expect("select must succeed") {
        SparqlResult::Select(r) => r
            .results
            .bindings
            .iter()
            .filter_map(|row| row.get(var).map(|b| b.value.clone()))
            .collect(),
        SparqlResult::Ask(_) => panic!("expected SELECT result, got ASK"),
    }
}

/// T-QEC-D05: DELETE WHERE removes all matching triples; SELECT no longer
/// returns them, while non-matching triples survive.
#[tokio::test]
async fn delete_where_removes_matching_triples() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update(
        "INSERT DATA { \
            <http://ex/a> <http://ex/p> \"one\" . \
            <http://ex/b> <http://ex/p> \"two\" . \
            <http://ex/c> <http://ex/q> \"three\" }",
        KS,
    )
    .await
    .expect("insert data");

    // Sanity: all three are present.
    let before = select_count(&eng, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await;
    assert_eq!(
        before, 3,
        "all three triples should be present before delete"
    );

    // Delete every triple whose predicate is :p (two of them).
    let res = eng
        .execute_update("DELETE WHERE { ?s <http://ex/p> ?o }", KS)
        .await
        .expect("delete where");
    assert_eq!(res.triples_deleted, 2, "two :p triples must be deleted");

    // SELECT no longer returns the :p triples.
    let p_left = select_count(&eng, "SELECT ?s ?o WHERE { ?s <http://ex/p> ?o }").await;
    assert_eq!(p_left, 0, "no :p triples may survive the DELETE WHERE");

    // The :q triple survives.
    let total = select_count(&eng, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await;
    assert_eq!(total, 1, "the non-matching :q triple must survive");
}

/// T-QEC-D06 (part 1): DELETE/INSERT … WHERE replaces matched data per
/// SPARQL 1.1 — the old object is gone, the new object is present.
#[tokio::test]
async fn delete_insert_where_replaces_object() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update(
        "INSERT DATA { <http://ex/alice> <http://ex/age> \"30\" }",
        KS,
    )
    .await
    .expect("insert data");

    // Replace the age object: delete the old, insert the new, bound by WHERE.
    eng.execute_update(
        "DELETE { ?s <http://ex/age> ?old } \
         INSERT { ?s <http://ex/age> \"31\" } \
         WHERE  { ?s <http://ex/age> ?old }",
        KS,
    )
    .await
    .expect("delete/insert where");

    // Exactly one age triple remains, and its object is the new value "31".
    // (Asserting on the object set is robust against the planner's PredicateScan
    // not constraining a bound object literal.)
    let ages = select_values(&eng, "SELECT ?s ?o WHERE { ?s <http://ex/age> ?o }", "o").await;
    assert_eq!(
        ages,
        vec!["31".to_string()],
        "old age \"30\" must be deleted and only the new \"31\" must remain"
    );
}

/// T-QEC-D06 (part 2): CLEAR GRAPH <g> removes every triple in the target
/// graph; a subsequent SELECT returns nothing.
#[tokio::test]
async fn clear_graph_removes_all_triples() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update(
        "INSERT DATA { \
            <http://ex/a> <http://ex/p> \"1\" . \
            <http://ex/b> <http://ex/q> \"2\" . \
            <http://ex/c> <http://ex/r> \"3\" }",
        KS,
    )
    .await
    .expect("insert data");

    assert_eq!(
        select_count(&eng, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await,
        3,
        "three triples present before CLEAR"
    );

    // CLEAR DEFAULT — clears the default graph (the keyspace's only graph).
    eng.execute_update("CLEAR DEFAULT", KS)
        .await
        .expect("clear graph");

    assert_eq!(
        select_count(&eng, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await,
        0,
        "CLEAR GRAPH must remove every triple in the graph"
    );
}

/// DROP behaves like CLEAR over the target graph: it tombstones every triple.
#[tokio::test]
async fn drop_default_removes_all_triples() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update(
        "INSERT DATA { \
            <http://ex/a> <http://ex/p> \"1\" . \
            <http://ex/b> <http://ex/q> \"2\" }",
        KS,
    )
    .await
    .expect("insert data");

    let res = eng
        .execute_update("DROP DEFAULT", KS)
        .await
        .expect("drop default");
    assert_eq!(res.triples_deleted, 2, "DROP must tombstone all triples");

    assert_eq!(
        select_count(&eng, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await,
        0,
        "DROP must leave the graph empty"
    );
}

/// No phantom deletion: CLEAR GRAPH of a graph IRI that does not match this
/// keyspace's graph must delete nothing and leave existing data intact
/// (URS-QEC-X01 — never fake a mutation it did not perform).
#[tokio::test]
async fn clear_non_matching_named_graph_deletes_nothing() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update("INSERT DATA { <http://ex/a> <http://ex/p> \"1\" }", KS)
        .await
        .expect("insert data");

    let res = eng
        .execute_update("CLEAR GRAPH <http://other.example/graph>", KS)
        .await
        .expect("clear non-matching graph is a valid no-op");
    assert_eq!(
        res.triples_deleted, 0,
        "clearing a graph this keyspace does not hold must delete nothing"
    );

    assert_eq!(
        select_count(&eng, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await,
        1,
        "the existing triple must survive an unrelated CLEAR GRAPH"
    );
}

/// Fail-loud: operations that are genuinely unimplemented must return a clear
/// error, never a fake success (URS-QEC-X01).
#[tokio::test]
async fn unimplemented_load_fails_loud() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    let err = eng
        .execute_update("LOAD <http://example.org/data.ttl>", KS)
        .await
        .expect_err("LOAD must fail loud, not fake success");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("load") || msg.contains("not") || msg.contains("unsupported"),
        "error must name the unimplemented op: {msg}"
    );
}
