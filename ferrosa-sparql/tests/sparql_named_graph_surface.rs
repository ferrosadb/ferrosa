//! The named-graph SPARQL surface the bench workload drives.
//!
//! The bench (`ferrosa-bench`, `urn:ferrosa-bench:RUNID`) issues, over the
//! deployed keyspace `"rdf"`:
//!
//!   1. `INSERT DATA { GRAPH <g> { <s> <urn:bench:label> "value-N" } }`
//!   2. `DELETE WHERE { GRAPH <g> { ?s ?p ?o } }`
//!   3. `SELECT ?o WHERE { GRAPH <g> { <s> <urn:bench:label> ?o } }`
//!
//! These tests pin the three invariants the workload depends on:
//!
//! - **Named-graph scoping** — a triple inserted into `GRAPH <g>` is visible to
//!   a SELECT over `<g>` and to neither a different named graph nor the default
//!   graph.
//! - **Delete is exact** — `DELETE WHERE { GRAPH <g> { ?s ?p ?o } }` removes
//!   every triple of `<g>` and nothing from any other graph.
//! - **Lookup completeness** — the bounded point lookup returns the inserted
//!   binding, not an empty result.
//!
//! The keyspace is `"rdf"` and the config is the shipped default, exactly as
//! `main.rs` and the HTTP surface build the engine — not a workaround keyspace.

use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_sparql::engine::{SparqlConfig, SparqlEngine, SparqlResult};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use tempfile::TempDir;

const KS: &str = "rdf";

/// A named graph IRI in the shape the bench uses.
const G: &str = "urn:ferrosa-bench:RUNID";
/// A second named graph, to prove scoping isolates graphs from each other.
const G2: &str = "urn:ferrosa-bench:OTHER";

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
        cache_hot_window_secs: 900,
    };
    let storage = Arc::new(StorageEngine::new(config, None).unwrap());
    let write_path = Arc::new(WritePath::direct(Arc::clone(&storage)));
    (storage, write_path, dir)
}

fn engine(storage: Arc<StorageEngine>, write_path: Arc<WritePath>) -> SparqlEngine {
    // The shipped config, addressed with the HTTP surface's default keyspace.
    SparqlEngine::new(storage, write_path, SparqlConfig::default())
}

/// Collect the bound values of `var` for a SELECT.
async fn select_values(eng: &SparqlEngine, query: &str, var: &str) -> Vec<String> {
    match eng.execute(query, KS).await.expect("select must succeed") {
        SparqlResult::Select(r) => r
            .results
            .bindings
            .iter()
            .filter_map(|row| row.get(var).map(|b| b.value.clone()))
            .collect(),
        other => panic!("expected SELECT result, got {other:?}"),
    }
}

/// Invariant: named-graph scoping + lookup completeness.
///
/// A triple inserted into `GRAPH <g>` is visible to the bounded point lookup
/// over `<g>` (the exact shape the bench issues), and to no other graph.
#[tokio::test]
async fn insert_into_a_named_graph_is_visible_to_its_point_lookup_and_nothing_else() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    let ins = format!(
        "INSERT DATA {{ GRAPH <{G}> {{ \
         <urn:bench:{G}:s:1> <urn:bench:label> \"value-1\" . \
         <urn:bench:{G}:s:2> <urn:bench:label> \"value-2\" }} }}"
    );
    let res = eng
        .execute_update(&ins, KS)
        .await
        .expect("INSERT DATA into a named graph must succeed");
    assert_eq!(
        res.triples_inserted, 2,
        "both inserted triples must be counted"
    );

    // 3. the bounded point lookup the bench issues, over the SAME graph.
    let in_g = select_values(
        &eng,
        &format!(
            "SELECT ?o WHERE {{ GRAPH <{G}> {{ <urn:bench:{G}:s:1> <urn:bench:label> ?o }} }}"
        ),
        "o",
    )
    .await;
    assert_eq!(
        in_g,
        vec!["value-1".to_string()],
        "the point lookup over the inserted named graph must return the inserted binding"
    );

    // The same subject/predicate in a DIFFERENT named graph must be invisible.
    let in_g2 = select_values(
        &eng,
        &format!(
            "SELECT ?o WHERE {{ GRAPH <{G2}> {{ <urn:bench:{G}:s:1> <urn:bench:label> ?o }} }}"
        ),
        "o",
    )
    .await;
    assert!(
        in_g2.is_empty(),
        "a triple in <{G}> must not be visible from <{G2}>, got {in_g2:?}"
    );

    // Nor in the default graph.
    let in_default = select_values(
        &eng,
        &format!("SELECT ?o WHERE {{ <urn:bench:{G}:s:1> <urn:bench:label> ?o }}"),
        "o",
    )
    .await;
    assert!(
        in_default.is_empty(),
        "a triple in <{G}> must not be visible from the default graph, got {in_default:?}"
    );

    // Nor to an UN-CLAUSED full scan, which reads the default graph only: a
    // named-graph triple must never leak into it.
    let unclaused = select_values(&eng, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }", "s").await;
    assert!(
        unclaused.is_empty(),
        "an un-claused full scan must read the default graph only, got {unclaused:?}"
    );
}

/// Invariant: an un-claused `DELETE WHERE { ?s ?p ?o }` clears the DEFAULT graph
/// and leaves every named graph intact.
///
/// Named graphs are separate partitions, so the delete must not reach them —
/// otherwise enabling named graphs would turn a plain delete into a
/// whole-store wipe.
#[tokio::test]
async fn unclaused_delete_where_clears_only_the_default_graph() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    let seed = format!(
        "INSERT DATA {{ \
         GRAPH <{G}> {{ <urn:bench:{G}:a> <urn:bench:label> \"g\" }} \
         <urn:bench:default:1> <urn:bench:label> \"default\" }}"
    );
    eng.execute_update(&seed, KS).await.expect("seed");

    let res = eng
        .execute_update("DELETE WHERE { ?s ?p ?o }", KS)
        .await
        .expect("un-claused DELETE WHERE");
    assert_eq!(
        res.triples_deleted, 1,
        "only the default-graph triple must go"
    );

    let named_after = select_values(
        &eng,
        &format!("SELECT ?o WHERE {{ GRAPH <{G}> {{ ?s <urn:bench:label> ?o }} }}"),
        "o",
    )
    .await;
    assert_eq!(
        named_after,
        vec!["g".to_string()],
        "the named graph must survive an un-claused default-graph delete"
    );
}

/// Invariant: delete is exact.
///
/// `DELETE WHERE { GRAPH <g> { ?s ?p ?o } }` removes every triple of `<g>` and
/// leaves every other graph untouched.
#[tokio::test]
async fn delete_where_over_a_named_graph_removes_only_that_graph() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    let seed = format!(
        "INSERT DATA {{ \
         GRAPH <{G}> {{ <urn:bench:{G}:a> <urn:bench:label> \"g-a\" . <urn:bench:{G}:b> <urn:bench:label> \"g-b\" }} \
         GRAPH <{G2}> {{ <urn:bench:{G2}:a> <urn:bench:label> \"g2-a\" }} \
         <urn:bench:default:1> <urn:bench:label> \"default\" }}"
    );
    eng.execute_update(&seed, KS).await.expect("seed");

    let res = eng
        .execute_update(
            &format!("DELETE WHERE {{ GRAPH <{G}> {{ ?s ?p ?o }} }}"),
            KS,
        )
        .await
        .expect("DELETE WHERE over a named graph must succeed");
    assert_eq!(
        res.triples_deleted, 2,
        "exactly <{G}>'s two triples must be deleted"
    );

    // <g> is empty…
    let g_after = select_values(
        &eng,
        &format!("SELECT ?s WHERE {{ GRAPH <{G}> {{ ?s ?p ?o }} }}"),
        "s",
    )
    .await;
    assert!(
        g_after.is_empty(),
        "<{G}> must be empty after the delete, got {g_after:?}"
    );

    // …and <g2> and the default graph survive intact.
    let g2_after = select_values(
        &eng,
        &format!("SELECT ?s WHERE {{ GRAPH <{G2}> {{ ?s ?p ?o }} }}"),
        "s",
    )
    .await;
    assert_eq!(
        g2_after,
        vec![format!("urn:bench:{G2}:a")],
        "the other named graph must survive the delete"
    );
    // …and the default graph is untouched.
    let default_present = select_values(
        &eng,
        "SELECT ?o WHERE { <urn:bench:default:1> <urn:bench:label> ?o }",
        "o",
    )
    .await;
    assert_eq!(
        default_present,
        vec!["default".to_string()],
        "the default-graph triple must survive a named-graph delete"
    );
}

/// `DELETE DATA { GRAPH <g> { … } }` removes exactly the named ground triple.
#[tokio::test]
async fn delete_data_from_a_named_graph_removes_exactly_that_triple() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    let seed = format!(
        "INSERT DATA {{ GRAPH <{G}> {{ <urn:bench:{G}:a> <urn:bench:label> \"a\" . <urn:bench:{G}:b> <urn:bench:label> \"b\" }} }}"
    );
    eng.execute_update(&seed, KS).await.expect("seed");

    eng.execute_update(
        &format!("DELETE DATA {{ GRAPH <{G}> {{ <urn:bench:{G}:a> <urn:bench:label> \"a\" }} }}"),
        KS,
    )
    .await
    .expect("DELETE DATA from a named graph must succeed");

    let remaining = select_values(
        &eng,
        &format!("SELECT ?o WHERE {{ GRAPH <{G}> {{ ?s <urn:bench:label> ?o }} }}"),
        "o",
    )
    .await;
    assert_eq!(
        remaining,
        vec!["b".to_string()],
        "only the named ground triple must be removed"
    );
}
