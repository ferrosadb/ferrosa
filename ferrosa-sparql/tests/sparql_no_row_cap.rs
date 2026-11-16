//! No server-side row cap: a SPARQL read is a stream, not a bounded buffer.
//!
//! The executor used to carry `DEFAULT_MAX_ROWS = 100_000` and refuse any scan
//! or operator buffer past it. That bound existed only because every operator
//! materialized its solutions; with solutions flowing as a stream (and ORDER
//! BY / DISTINCT / closure state spilling to disk) a large result is just a
//! large stream. These tests seed one row PAST the old cap and require the
//! complete, correct answer from every shape that used to trip it: a plain
//! scan, ORDER BY, DISTINCT, and a property-path closure.
//! Correctness: complete results past 100_000 storage rows, no truncation.
//! Last revised: 2026-11-16
//! Last changed: Pin the no-cap contract for scans, ORDER BY, DISTINCT and paths.

use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_sparql::engine::{SparqlConfig, SparqlEngine, SparqlResult};
use ferrosa_storage::{StorageEngine, StorageEngineConfig};

const KS: &str = "default";

/// One row past the executor's former `DEFAULT_MAX_ROWS` (100_000).
const ROWS_PAST_OLD_BOUND: usize = 100_001;

/// Triples written per `INSERT DATA` request while seeding.
const SEED_BATCH: usize = 5_000;

fn fixture() -> (SparqlEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageEngineConfig::test_config(dir.path());
    config.commit_log.segment_size = 64 * 1024 * 1024;
    // Seeding 100k triples must not fsync per write; durability is not under test.
    config.commit_log.sync_strategy = ferrosa_storage::SyncStrategyConfig::Periodic {
        sync_interval: std::time::Duration::from_secs(1),
    };
    let storage = Arc::new(StorageEngine::new(config, None).unwrap());
    let path = Arc::new(WritePath::direct(Arc::clone(&storage)));
    let config = SparqlConfig {
        default_graph: KS.into(),
        ..Default::default()
    };
    (SparqlEngine::new(storage, path, config), dir)
}

/// Write `<urn:n{i}> <urn:next> <urn:n{i+1}>` for every `i < n`: a chain of
/// `n` edges, one partition per subject, whose objects are all distinct.
async fn seed_chain(engine: &SparqlEngine, n: usize) {
    for start in (0..n).step_by(SEED_BATCH) {
        let mut data = String::from("INSERT DATA {");
        for i in start..(start + SEED_BATCH).min(n) {
            data.push_str(&format!(" <urn:n{i}> <urn:next> <urn:n{}> .", i + 1));
        }
        data.push('}');
        engine.execute_update(&data, KS).await.expect("seed insert");
    }
}

async fn select(
    engine: &SparqlEngine,
    query: &str,
) -> Vec<std::collections::HashMap<String, String>> {
    let result = engine
        .execute(query, KS)
        .await
        .unwrap_or_else(|e| panic!("query must not fail past the old row bound: {e}\n{query}"));
    let SparqlResult::Select(result) = result else {
        panic!("SELECT must return a SELECT result");
    };
    result
        .results
        .bindings
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|(k, v)| (k, v.scalar_value().unwrap().to_owned()))
                .collect()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scans_order_by_and_distinct_past_the_old_bound_are_complete() {
    let (engine, _dir) = fixture();
    seed_chain(&engine, ROWS_PAST_OLD_BOUND).await;

    // A plain scan returns every row, not an error and not a prefix.
    let rows = select(&engine, "SELECT ?s ?o WHERE { ?s <urn:next> ?o }").await;
    assert_eq!(rows.len(), ROWS_PAST_OLD_BOUND, "every edge must come back");

    // ORDER BY sees every row: the lexically greatest subject is n99999, and
    // the smallest is n0.
    let rows = select(
        &engine,
        "SELECT ?s WHERE { ?s <urn:next> ?o } ORDER BY DESC(?s) LIMIT 2",
    )
    .await;
    let top: Vec<&str> = rows.iter().map(|r| r["s"].as_str()).collect();
    assert_eq!(top, vec!["urn:n99999", "urn:n99998"]);
    let rows = select(
        &engine,
        "SELECT ?s WHERE { ?s <urn:next> ?o } ORDER BY ?s LIMIT 1",
    )
    .await;
    assert_eq!(rows[0]["s"], "urn:n0");

    // DISTINCT over a column with one value per row keeps every row; over a
    // constant column it collapses to one.
    let rows = select(&engine, "SELECT DISTINCT ?o WHERE { ?s <urn:next> ?o }").await;
    assert_eq!(rows.len(), ROWS_PAST_OLD_BOUND);
    let rows = select(&engine, "SELECT DISTINCT ?p WHERE { ?s ?p ?o }").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["p"], "urn:next");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn property_path_closure_past_the_old_bound_is_complete() {
    let (engine, _dir) = fixture();
    seed_chain(&engine, ROWS_PAST_OLD_BOUND).await;

    // n0 reaches every later node of the chain exactly once.
    let rows = select(&engine, "SELECT ?o WHERE { <urn:n0> <urn:next>+ ?o }").await;
    assert_eq!(rows.len(), ROWS_PAST_OLD_BOUND);
    let mut seen = std::collections::HashSet::new();
    for row in &rows {
        assert!(seen.insert(row["o"].clone()), "duplicate {}", row["o"]);
    }
    assert!(seen.contains(&format!("urn:n{ROWS_PAST_OLD_BOUND}")));
    assert!(!seen.contains("urn:n0"), "OneOrMore excludes the start");

    // ZeroOrMore from the last-but-one node includes itself and the tail.
    let start = ROWS_PAST_OLD_BOUND - 1;
    let query = format!("SELECT ?o WHERE {{ <urn:n{start}> <urn:next>* ?o }}");
    let mut tail: Vec<String> = select(&engine, &query)
        .await
        .into_iter()
        .map(|r| r["o"].clone())
        .collect();
    tail.sort();
    assert_eq!(
        tail,
        vec![
            format!("urn:n{start}"),
            format!("urn:n{ROWS_PAST_OLD_BOUND}")
        ]
    );
}
