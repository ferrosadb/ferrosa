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
//! These are asserted WITHOUT timing or instrumentation. The executor's row
//! bound (`ExecutionLimits::max_rows`) is set below the table size, which makes
//! "did this query read the whole table?" directly observable: a query that
//! reads past the bound errors, and a query that stops early succeeds. A test
//! that cannot tell the difference is not testing boundedness.
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
use ferrosa_sparql::error::SparqlError;
use ferrosa_sparql::executor::{self, ExecutionLimits, DEFAULT_MAX_ROWS};
use ferrosa_sparql::planner;
use ferrosa_sparql::results::Binding;
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use std::collections::HashMap;
use tempfile::TempDir;

const KS: &str = "default";

/// Rows written by [`Fixture::seed`]. Chosen so a bound of
/// [`BOUND`] sits strictly between a `LIMIT` window and the table size.
const TABLE_ROWS: usize = 20;

/// The row bound under test — well below `TABLE_ROWS`, so any query that reads
/// the whole table trips it.
const BOUND: usize = 8;

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

    /// Run `query` through `executor::execute` under an explicit row bound.
    async fn run(
        &self,
        query: &str,
        max_rows: usize,
    ) -> Result<Vec<HashMap<String, Binding>>, SparqlError> {
        let parsed = spargebra::SparqlParser::new()
            .parse_query(query)
            .expect("query must parse");
        let plan = planner::plan_query(&parsed, KS).expect("query must plan");
        Ok(
            executor::execute(&plan, &self.write_path, &ExecutionLimits { max_rows })
                .await?
                .results
                .bindings,
        )
    }
}

// =======================================================================
// I6 — Complete result, or an error. Never a silent truncation.
// =======================================================================

/// I6: a scan that cannot finish within the row bound must ERROR. Returning the
/// first `BOUND` rows and calling it the answer is indistinguishable from a
/// correct short result, which is exactly the failure mode the old
/// `SCAN_ROW_CAP` warning pretended to guard against while truncating nothing.
#[tokio::test]
async fn scan_exceeding_the_row_bound_errors_instead_of_truncating() {
    let f = fixture();
    f.seed().await;

    let err = f
        .run("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", BOUND)
        .await
        .expect_err("a full scan of 20 rows under an 8-row bound must fail, not truncate");

    let msg = err.to_string();
    assert!(
        msg.contains(&BOUND.to_string()),
        "the error must name the bound that was crossed: {msg}"
    );
    assert!(
        msg.to_lowercase().contains("truncat"),
        "the error must say it is refusing to truncate, so an operator knows the \
         result is missing rather than empty: {msg}"
    );
}

/// I6: a scan that DOES fit within the bound returns the complete result and no
/// error. The bound must not fire early — a guard that rejects valid queries is
/// as wrong as one that truncates silently.
#[tokio::test]
async fn scan_within_the_row_bound_returns_the_complete_result() {
    let f = fixture();
    f.seed().await;

    let rows = f
        .run("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", TABLE_ROWS)
        .await
        .expect("a scan that fits inside the bound must succeed");

    assert_eq!(
        rows.len(),
        TABLE_ROWS,
        "the result must be complete, not clipped at the bound"
    );
}

/// I6: ORDER BY is a blocking operator — it must see every solution before it
/// can emit the first. It may therefore buffer, but it must respect the bound
/// and fail loud past it rather than sorting a clipped window and presenting it
/// as the global order.
#[tokio::test]
async fn order_by_over_a_scan_exceeding_the_bound_errors() {
    let f = fixture();
    f.seed().await;

    let err = f
        .run(
            "SELECT ?o WHERE { ?s <http://ex/p> ?o } ORDER BY ?o LIMIT 3",
            BOUND,
        )
        .await
        .expect_err(
            "ORDER BY must see all 20 solutions to know the smallest 3; it may not \
             sort the first 8 and call the answer global",
        );

    assert!(err.to_string().contains(&BOUND.to_string()));
}

/// I6: DISTINCT is likewise blocking — it cannot know a solution is a duplicate
/// without having seen the earlier one.
#[tokio::test]
async fn distinct_over_a_scan_exceeding_the_bound_errors() {
    let f = fixture();
    f.seed().await;

    let err = f
        .run("SELECT DISTINCT ?p WHERE { ?s ?p ?o } LIMIT 1", BOUND)
        .await
        .expect_err("DISTINCT must not deduplicate a clipped window and report success");

    assert!(err.to_string().contains(&BOUND.to_string()));
}

/// I6: a FILTER drops solutions AFTER they are bound, so a `LIMIT` above a
/// filter cannot be pushed into the scan — stopping at `n` bound rows could
/// return fewer than `n` results. The scan must therefore run to completion (and
/// trip the bound) rather than quietly returning a short answer.
#[tokio::test]
async fn filtered_limit_does_not_push_down_and_stay_short() {
    let f = fixture();
    f.seed().await;

    let err = f
        .run(
            "SELECT ?o WHERE { ?s ?p ?o . FILTER(?o = \"object-019\") } LIMIT 1",
            BOUND,
        )
        .await
        .expect_err(
            "the matching triple is the last one written; a pushed-down LIMIT would \
             stop after 1 bound row and return nothing, so LIMIT must not push \
             through a FILTER",
        );

    assert!(err.to_string().contains(&BOUND.to_string()));
}

// =======================================================================
// I7 — A bounded query does bounded work.
// =======================================================================

/// I7: `LIMIT n` must stop reading once it has `n` solutions.
///
/// The bound is 8 rows and the table is 20. A `LIMIT 3` full scan that reads
/// only what it needs touches 3 rows and succeeds; one that materializes the
/// table first reads 20 and trips the bound. The unbounded form of the same
/// query is asserted to fail in the same test, which is what proves the success
/// above comes from stopping early rather than from the bound being inert.
#[tokio::test]
async fn limit_stops_the_scan_instead_of_reading_the_whole_table() {
    let f = fixture();
    f.seed().await;

    let rows = f
        .run("SELECT ?s WHERE { ?s ?p ?o } LIMIT 3", BOUND)
        .await
        .expect("LIMIT 3 must read ~3 rows, not all 20");
    assert_eq!(rows.len(), 3);

    f.run("SELECT ?s WHERE { ?s ?p ?o }", BOUND)
        .await
        .expect_err(
            "control: the same scan without a LIMIT must trip the bound — otherwise \
             the assertion above proves nothing about early termination",
        );
}

/// I7: OFFSET rows are read and discarded, so the scan must produce
/// `offset + limit` solutions — no fewer (which would lose rows) and, within
/// the bound, no more.
#[tokio::test]
async fn limit_with_offset_reads_offset_plus_limit_rows() {
    let f = fixture();
    f.seed().await;

    let rows = f
        .run("SELECT ?s WHERE { ?s ?p ?o } LIMIT 2 OFFSET 4", BOUND)
        .await
        .expect("OFFSET 4 LIMIT 2 needs 6 rows, which fits inside the 8-row bound");
    assert_eq!(
        rows.len(),
        2,
        "the window must still be exactly 2 rows wide"
    );

    f.run("SELECT ?s WHERE { ?s ?p ?o } LIMIT 2 OFFSET 12", BOUND)
        .await
        .expect_err(
            "OFFSET 12 LIMIT 2 needs 14 rows, past the 8-row bound: it must fail \
             loud rather than return a window built from the rows it managed to read",
        );
}

/// I7: an ASK query is planned with `LIMIT 1`, so it must terminate after the
/// first matching row instead of scanning the store to answer a yes/no.
#[tokio::test]
async fn ask_terminates_after_the_first_match() {
    let f = fixture();
    f.seed().await;

    let result = f.engine.execute("ASK { ?s ?p ?o }", KS).await;

    match result.expect("ASK must not read the whole table") {
        ferrosa_sparql::engine::SparqlResult::Ask(a) => assert!(a.boolean),
        other => panic!("expected an ASK result, got {other:?}"),
    }
}

/// A `LIMIT` larger than the table is not a pushdown opportunity, but it must
/// still return everything rather than erroring — the bound is about work done,
/// not about the number the user typed.
#[tokio::test]
async fn limit_larger_than_the_table_returns_everything() {
    let f = fixture();
    f.seed().await;

    let rows = f
        .run("SELECT ?s WHERE { ?s ?p ?o } LIMIT 1000", TABLE_ROWS)
        .await
        .expect("a LIMIT above the table size is satisfied by the whole table");

    assert_eq!(rows.len(), TABLE_ROWS);
}

/// The default bound is a real number the engine actually enforces, not a
/// constant nothing reads. `SparqlConfig::default()` must carry it.
#[tokio::test]
async fn default_config_carries_the_enforced_row_bound() {
    assert_eq!(
        SparqlConfig::default().max_rows,
        DEFAULT_MAX_ROWS,
        "the shipped config must use the executor's documented default bound"
    );
    assert_eq!(
        ExecutionLimits::default().max_rows,
        DEFAULT_MAX_ROWS,
        "ExecutionLimits::default must agree with the config default"
    );
}
