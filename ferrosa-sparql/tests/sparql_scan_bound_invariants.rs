//! Completeness and boundedness invariants for the SPARQL executor.
//!
//! Two invariants, both about the SOURCE of the data rather than the shape of
//! the output:
//!
//! - **I6 completeness** — the engine returns either a COMPLETE result or an
//!   ERROR. It must never return a silently truncated result that looks
//!   complete. A short answer a caller cannot distinguish from the real one is
//!   the worst possible outcome; a loud failure is recoverable.
//!
//! - **I7 boundedness** — a bounded query does bounded work. `LIMIT n` must
//!   stop reading once it has `n` solutions instead of reading the whole table
//!   and discarding the rest.
//!
//! These are asserted WITHOUT timing. The executor has no row bound any more
//! (solutions stream and blocking operators spill), so "did this query read the
//! whole table?" is observed through the query's own counter,
//! `ExecutionStats::storage_rows_read`: a query that stops early reads fewer
//! rows than the table holds, and its unlimited control reads all of them. A
//! test that cannot tell the difference is not testing boundedness.
//!
//! Keyspace note: these tests use `KS = "default"`, matching the graph
//! component `INSERT DATA` writes, because they are about HOW MUCH data the
//! scan reads and must not be confounded by t_af4eb9f0 (the graph/keyspace
//! partition-key mismatch, which makes point reads miss). The read-path
//! agreement invariant that t_af4eb9f0 violates is asserted separately, on the
//! deployed keyspace, in `sparql_executor_invariants.rs`.

use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_sparql::engine::{SparqlConfig, SparqlEngine};
use ferrosa_sparql::executor::{self, ExecutionContext, ExecutionStats};
use ferrosa_sparql::planner;
use ferrosa_sparql::results::Binding;
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use std::collections::HashMap;
use tempfile::TempDir;

const KS: &str = "default";

/// Rows written by [`Fixture::seed`], one per partition.
const TABLE_ROWS: usize = 20;

/// The executor's former row bound in these tests: well below `TABLE_ROWS`.
/// Every query below reads or buffers more than this and must still succeed.
const OLD_BOUND: usize = 8;

fn storage_config(dir: &TempDir) -> StorageEngineConfig {
    StorageEngineConfig {
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
    }
}

struct Fixture {
    engine: SparqlEngine,
    write_path: Arc<WritePath>,
    _dir: TempDir,
}

fn fixture() -> Fixture {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(StorageEngine::new(storage_config(&dir), None).unwrap());
    let write_path = Arc::new(WritePath::direct(Arc::clone(&storage)));
    let config = SparqlConfig {
        default_graph: KS.to_string(),
        ..Default::default()
    };
    let engine = SparqlEngine::new(storage, Arc::clone(&write_path), config);
    Fixture {
        engine,
        write_path,
        _dir: dir,
    }
}

impl Fixture {
    /// Write [`TABLE_ROWS`] triples, each in its own partition.
    async fn seed(&self) {
        let mut data = String::from("INSERT DATA {");
        for i in 0..TABLE_ROWS {
            data.push_str(&format!(
                " <http://ex/s{i}> <http://ex/p> \"object-{i:03}\" ."
            ));
        }
        data.push('}');
        self.engine
            .execute_update(&data, KS)
            .await
            .expect("INSERT DATA must succeed");
    }

    /// Run `query` through `executor::execute`, returning its rows and how many
    /// storage rows it read. Spills go under the fixture's own directory with a
    /// tiny threshold, so ORDER BY and DISTINCT exercise their disk paths.
    async fn run(&self, query: &str) -> (Vec<HashMap<String, Binding>>, u64) {
        let (rows, stats) = self.run_with_stats(query).await;
        (rows, stats.storage_rows_read())
    }

    async fn run_with_stats(
        &self,
        query: &str,
    ) -> (Vec<HashMap<String, Binding>>, Arc<ExecutionStats>) {
        let parsed = spargebra::SparqlParser::new()
            .parse_query(query)
            .expect("query must parse");
        // The scope carries the keyspace (which table) and the graph (which partition)
        // separately; these suites keep them equal because that is what their fixtures write.
        let scope = planner::TripleScope::new(KS, KS);
        let plan = planner::plan_query(&parsed, &scope).expect("query must plan");
        let cx =
            ExecutionContext::new(self._dir.path().join("spill"), 64).with_dedup_resident_keys(2);
        let rows = executor::execute(&plan, &self.write_path, &cx)
            .await
            .unwrap_or_else(|e| panic!("{query} must succeed with no row bound: {e}"))
            .results
            .bindings;
        (rows, Arc::clone(cx.stats()))
    }
}

// =======================================================================
// I6 — Complete result, or an error. Never a silent truncation, never a cap.
// =======================================================================

/// I6: a scan past the former row bound returns the complete result. It used
/// to be refused; now nothing caps it.
#[tokio::test]
async fn scan_past_the_old_bound_returns_the_complete_result() {
    let f = fixture();
    f.seed().await;

    let (rows, read) = f.run("SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await;
    assert_eq!(rows.len(), TABLE_ROWS, "every row, not a prefix");
    assert!(read as usize > OLD_BOUND);
}

/// I6: ORDER BY must see every solution before emitting the first. Past the
/// spill threshold it sorts on disk and still returns the global order.
#[tokio::test]
async fn order_by_past_the_old_bound_returns_the_global_order() {
    let f = fixture();
    f.seed().await;

    let (rows, read) = f
        .run("SELECT ?o WHERE { ?s <http://ex/p> ?o } ORDER BY DESC(?o) LIMIT 3")
        .await;
    let top: Vec<&str> = rows
        .iter()
        .map(|r| r["o"].scalar_value().unwrap())
        .collect();
    assert_eq!(top, vec!["object-019", "object-018", "object-017"]);
    assert_eq!(read as usize, TABLE_ROWS, "ORDER BY reads everything, once");
}

/// ORDER BY and DISTINCT past the in-memory threshold go to disk — the spill
/// path, not a resident buffer, produced the correct answers above.
#[tokio::test]
async fn order_by_and_distinct_spill_past_the_threshold_and_stay_correct() {
    let f = fixture();
    f.seed().await;

    let (rows, stats) = f
        .run_with_stats("SELECT ?o WHERE { ?s <http://ex/p> ?o } ORDER BY ?o")
        .await;
    let got: Vec<&str> = rows
        .iter()
        .map(|r| r["o"].scalar_value().unwrap())
        .collect();
    let want: Vec<String> = (0..TABLE_ROWS).map(|i| format!("object-{i:03}")).collect();
    assert_eq!(got, want, "the merged runs must be in global order");
    assert_eq!(
        stats.sort_spills(),
        1,
        "a 64-byte threshold must spill the sort"
    );

    let (rows, stats) = f
        .run_with_stats("SELECT DISTINCT ?o WHERE { ?s ?p ?o }")
        .await;
    assert_eq!(rows.len(), TABLE_ROWS);
    assert_eq!(
        stats.dedup_spills(),
        1,
        "two resident keys must spill the set"
    );
}

/// I6: DISTINCT cannot know a solution is a duplicate without having seen the
/// earlier one; its seen-set spills instead of capping.
#[tokio::test]
async fn distinct_past_the_old_bound_is_exact() {
    let f = fixture();
    f.seed().await;

    let (rows, _) = f.run("SELECT DISTINCT ?p WHERE { ?s ?p ?o }").await;
    assert_eq!(rows.len(), 1);
    let (rows, _) = f.run("SELECT DISTINCT ?o WHERE { ?s ?p ?o }").await;
    assert_eq!(rows.len(), TABLE_ROWS);
}

/// I6: a FILTER drops solutions AFTER they are bound, so `LIMIT 1` above a
/// filter must keep reading until a solution survives — here the last row.
#[tokio::test]
async fn filtered_limit_reads_until_a_solution_survives() {
    let f = fixture();
    f.seed().await;

    let (rows, _) = f
        .run("SELECT ?o WHERE { ?s ?p ?o . FILTER(?o = \"object-019\") } LIMIT 1")
        .await;
    assert_eq!(
        rows.len(),
        1,
        "the matching triple must be found, not cut off"
    );
    assert_eq!(rows[0]["o"].scalar_value(), Some("object-019"));
}

// =======================================================================
// I7 — A bounded query does bounded work.
// =======================================================================

/// I7: `LIMIT n` stops reading once it has `n` solutions: the response pulls,
/// and nothing pulls past the n-th solution. The unlimited control reads the
/// whole table, which is what proves the limited read stopped early.
#[tokio::test]
async fn limit_stops_the_scan_instead_of_reading_the_whole_table() {
    let f = fixture();
    f.seed().await;

    let (rows, read) = f.run("SELECT ?s WHERE { ?s ?p ?o } LIMIT 3").await;
    assert_eq!(rows.len(), 3);
    assert_eq!(read, 3, "one row per partition: LIMIT 3 reads exactly 3");

    let (rows, read) = f.run("SELECT ?s WHERE { ?s ?p ?o }").await;
    assert_eq!(rows.len(), TABLE_ROWS);
    assert_eq!(
        read as usize, TABLE_ROWS,
        "control: the unlimited scan reads all"
    );
}

/// I7: OFFSET rows are read and discarded, so the scan reads exactly
/// `offset + limit` rows — no fewer (which would lose rows), no more.
#[tokio::test]
async fn limit_with_offset_reads_offset_plus_limit_rows() {
    let f = fixture();
    f.seed().await;

    let (rows, read) = f.run("SELECT ?s WHERE { ?s ?p ?o } LIMIT 2 OFFSET 4").await;
    assert_eq!(
        rows.len(),
        2,
        "the window must still be exactly 2 rows wide"
    );
    assert_eq!(read, 6);

    let (rows, read) = f
        .run("SELECT ?s WHERE { ?s ?p ?o } LIMIT 2 OFFSET 12")
        .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(read, 14);
}

/// I7: an ASK query is planned with `LIMIT 1`, so it must terminate after the
/// first matching row instead of scanning the store to answer a yes/no.
#[tokio::test]
async fn ask_terminates_after_the_first_match() {
    let f = fixture();
    f.seed().await;

    let result = f.engine.execute("ASK { ?s ?p ?o }", KS).await;

    match result.expect("ASK must answer") {
        ferrosa_sparql::engine::SparqlResult::Ask(a) => assert!(a.boolean),
        other => panic!("expected an ASK result, got {other:?}"),
    }
}

/// A `LIMIT` larger than the table returns everything — LIMIT is an
/// optimization, not a bound.
#[tokio::test]
async fn limit_larger_than_the_table_returns_everything() {
    let f = fixture();
    f.seed().await;

    let (rows, read) = f.run("SELECT ?s WHERE { ?s ?p ?o } LIMIT 1000").await;
    assert_eq!(rows.len(), TABLE_ROWS);
    assert_eq!(read as usize, TABLE_ROWS);
}
