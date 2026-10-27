//! The graph's object index: created at registration, consulted by the
//! object-bound access path.
//!
//! `executor`'s `ObjectScan` arm consults `rdf_triples_object_idx` when this
//! node declares it and otherwise streams a full table scan with filtering.
//! Nothing created the index, so every `?s ?p <o>` pattern read the whole
//! `rdf_triples` table of the keyspace it was aimed at, and the index-read arm
//! beside the fallback was written and unreachable.
//!
//! Three things have to hold for the fix to be real, and each has its own test:
//!
//! 1. registration DECLARES the index, and the object-bound query returns
//!    exactly the triples it stored — the VALUES, not a count;
//! 2. the query stops reading once the index has answered. The instrument is
//!    the executor's own row counter: `Scan::feed` counts every storage row at
//!    the source and errors past `ExecutionLimits::max_rows`, so a table of 20
//!    rows under an 8-row bound is answerable ONLY by an index read. No timing
//!    and no new counter is involved;
//! 3. MIRROR CONTROL — a table with NO declared index must still stream the
//!    full scan, still answer with the right values, and still say so. Without
//!    it this defect could be "fixed" by deleting the fallback (an undeclared
//!    index is refused by `read_by_index_each_after`, so those reads would then
//!    fail loud) or by never consulting the index at all.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use ferrosa_cluster::write_path::WritePath;
use ferrosa_sparql::engine::{SparqlConfig, SparqlEngine, SparqlResult};
use ferrosa_sparql::executor::{self, ExecutionLimits};
use ferrosa_sparql::planner;
use ferrosa_sparql::results::Binding;
use ferrosa_sparql::triple_store::triples_table_id;
use ferrosa_storage::index::IndexBackendConfig;
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use tempfile::TempDir;

/// The index the `ObjectScan` arm reads through. Spelled out rather than
/// imported from `ferrosa_sparql::executor`: this test pins the NAME the
/// registration site and the executor must agree on, and an import of the same
/// constant would follow a rename silently.
const OBJECT_INDEX: &str = "rdf_triples_object_idx";

/// Keyspace and (via the fixture's config) graph component, so the two cannot
/// be confused by accident.
const KS: &str = "default";

/// Rows written by [`Fixture::seed`] — one triple per subject, so each is its
/// own partition.
const TABLE_ROWS: usize = 20;

/// The executor row bound under test: strictly below [`TABLE_ROWS`], so any
/// access path that reads the whole table trips it while an index read of one
/// row cannot.
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
        cache_hot_window_secs: 900,
    }
}

struct Fixture {
    storage: Arc<StorageEngine>,
    write_path: Arc<WritePath>,
    engine: SparqlEngine,
    config: SparqlConfig,
    _dir: TempDir,
}

/// `keyspace == default_graph`: rows land in the graph component the served
/// scope isolates on, so an index that ignored the graph would be visible here.
fn fixture(config: SparqlConfig) -> Fixture {
    fixture_with_index_backend(config, IndexBackendConfig::Local)
}

/// `fixture`, with the index build backend chosen by the caller.
///
/// `IndexBackendConfig::Off` builds no scheduler, so a declaration made over
/// SSTables that already exist marks every one of them pending and nothing
/// ever builds them. That is the backfill window a real node passes through —
/// the index is registered, incomplete, and known-incomplete by the tracker —
/// held still instead of racing a live builder.
fn fixture_with_index_backend(config: SparqlConfig, index_backend: IndexBackendConfig) -> Fixture {
    let dir = TempDir::new().unwrap();
    let mut storage_config = storage_config(&dir);
    storage_config.index_backend = index_backend;
    let storage = Arc::new(StorageEngine::new(storage_config, None).unwrap());
    let write_path = Arc::new(WritePath::direct(Arc::clone(&storage)));
    let engine = SparqlEngine::new(
        Arc::clone(&storage),
        Arc::clone(&write_path),
        config.clone(),
    );
    Fixture {
        storage,
        write_path,
        engine,
        config,
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

    /// Run `query` through the executor under an explicit row bound — the same
    /// public entry point `SparqlEngine::execute` drives, with the bound under
    /// test instead of the default one.
    ///
    /// Returns `(subject, predicate)` of every solution: the object is the
    /// pattern's own constant, so the pair IS the row's identity and content.
    async fn run_bounded(
        &self,
        query: &str,
        max_rows: usize,
    ) -> Result<Vec<(String, String)>, ferrosa_sparql::error::SparqlError> {
        let parsed = spargebra::SparqlParser::new()
            .parse_query(query)
            .expect("query must parse");
        let scope = planner::TripleScope::new(KS, KS);
        let plan = planner::plan_query(&parsed, &scope).expect("query must plan");
        let out = executor::execute(&plan, &self.write_path, &ExecutionLimits { max_rows }).await?;
        Ok(rows_of(&out.results.bindings))
    }

    /// Leave the object index DECLARED but NOT current, over the SSTables the
    /// seeded rows were flushed into, and hand back the engine a fresh
    /// registration boots.
    ///
    /// The shape is a node that comes up with data already on disk while the
    /// index it now declares is still backfilling:
    ///
    /// 1. the seeded rows are flushed into an SSTable;
    /// 2. the declaration is dropped, so the index is as absent as it was
    ///    before `SparqlEngine::new` declared it — the rows stay on disk;
    /// 3. `SparqlEngine::new` is run again over the SAME storage. That is the
    ///    real registration entry point: it declares the object index, finds
    ///    SSTables that predate the declaration, and marks them pending.
    ///
    /// The backend is `Off`, so nothing drains that pending set and the window
    /// does not close under the test. The tracker state the executor reads is
    /// the same either way — this only decides whether it is still there when
    /// the assertions run.
    fn leave_object_index_declared_but_not_current(&self) -> SparqlEngine {
        let table_id = triples_table_id(KS);
        self.storage
            .flush_all()
            .expect("the seeded rows must reach an SSTable, or there is no backfill to be pending");
        assert!(
            self.storage
                .drop_index(&table_id, OBJECT_INDEX)
                .expect("drop the declared index"),
            "the fixture's registration must have declared '{OBJECT_INDEX}' for this window to exist"
        );
        assert!(
            !self.storage.declares_index(&table_id, OBJECT_INDEX),
            "step 2 must leave the index undeclared, or step 3 declares nothing"
        );

        let engine = SparqlEngine::new(
            Arc::clone(&self.storage),
            Arc::clone(&self.write_path),
            self.config.clone(),
        );
        assert!(
            self.storage.declares_index(&table_id, OBJECT_INDEX),
            "the fresh registration must declare '{OBJECT_INDEX}' over the existing SSTables"
        );
        assert!(
            !self.storage.index_is_current(&table_id, OBJECT_INDEX),
            "the fixture is only meaningful while the declared index is known-incomplete"
        );
        engine
    }
}

/// `(subject, predicate)` of every solution, sorted. Every query in this file
/// binds its object as a pattern constant, so these two are the whole row.
fn rows_of(bindings: &[std::collections::HashMap<String, Binding>]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = bindings
        .iter()
        .map(|row| {
            (
                row["s"].scalar_value().unwrap().to_owned(),
                row["p"].scalar_value().unwrap().to_owned(),
            )
        })
        .collect();
    out.sort();
    out
}

/// Run an object-bound query through the served entry point and return the
/// `(subject, predicate)` pairs, as a set.
async fn served_object_rows(engine: &SparqlEngine, object: &str) -> HashSet<(String, String)> {
    let query = format!("SELECT ?s ?p WHERE {{ ?s ?p \"{object}\" }}");
    match engine
        .execute(&query, KS)
        .await
        .expect("select must succeed")
    {
        SparqlResult::Select(r) => r
            .results
            .bindings
            .iter()
            .map(|row| {
                (
                    row["s"].scalar_value().unwrap().to_owned(),
                    row["p"].scalar_value().unwrap().to_owned(),
                )
            })
            .collect(),
        SparqlResult::Ask(_) => panic!("expected SELECT result, got ASK"),
        SparqlResult::Graph(_) => panic!("expected SELECT result, got Graph"),
    }
}

fn pair(subject: &str, predicate: &str) -> (String, String) {
    (
        format!("http://ex/{subject}"),
        format!("http://ex/{predicate}"),
    )
}

/// [`served_object_rows`], with the failure left to the caller.
///
/// The backfill-window test has to LOOK at the failure: the defect it pins is
/// a read that refuses, and an `expect` would print the refusal without ever
/// comparing it to the rows the scan below would have returned.
async fn served_object_rows_or_error(
    engine: &SparqlEngine,
    object: &str,
) -> Result<HashSet<(String, String)>, String> {
    let query = format!("SELECT ?s ?p WHERE {{ ?s ?p \"{object}\" }}");
    match engine
        .execute(&query, KS)
        .await
        .map_err(|e| e.to_string())?
    {
        SparqlResult::Select(r) => Ok(r
            .results
            .bindings
            .iter()
            .map(|row| {
                (
                    row["s"].scalar_value().unwrap().to_owned(),
                    row["p"].scalar_value().unwrap().to_owned(),
                )
            })
            .collect()),
        SparqlResult::Ask(_) => Err("expected SELECT result, got ASK".into()),
        SparqlResult::Graph(_) => Err("expected SELECT result, got Graph".into()),
    }
}

// ── Log capture ─────────────────────────────────────────────────────────
//
// The mirror control has to show the fallback still SAYS it is scanning; a
// silent fallback is a fallback nobody can see in an incident.

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = LogWriter;
    fn make_writer(&'a self) -> Self::Writer {
        LogWriter(Arc::clone(&self.0))
    }
}

// =======================================================================
// 1 — registration declares the index, and the answer is the stored data.
// =======================================================================

/// The object-bound query returns exactly the triples stored, before and after
/// a flush, and the registration that made it possible declared the index.
///
/// RED as written: the VALUE half holds already (an absent index means the scan
/// answers correctly), so this test fails on the `declares_index` assertion —
/// the index the `ObjectScan` arm reads through is never created.
#[tokio::test]
async fn registration_declares_the_object_index_the_object_scan_reads() {
    let f = fixture(SparqlConfig {
        default_graph: KS.to_string(),
        ..Default::default()
    });

    // Four triples share the object "shared"; one of them carries a different
    // predicate, so a wrong access path that returned whole partitions or
    // ignored the object would be visible in the VALUES rather than only in a
    // count. A fifth triple holds the same object in a NAMED graph, which the
    // served scope must not read.
    f.engine
        .execute_update(
            "INSERT DATA { \
             <http://ex/a> <http://ex/p> \"shared\" . \
             <http://ex/b> <http://ex/p> \"shared\" . \
             <http://ex/c> <http://ex/p> \"shared\" . \
             <http://ex/d> <http://ex/q> \"shared\" . \
             <http://ex/e> <http://ex/p> \"other\" }",
            KS,
        )
        .await
        .expect("insert data");
    f.engine
        .execute_update(
            "INSERT DATA { GRAPH <http://ex/g2> { <http://ex/z> <http://ex/p> \"shared\" } }",
            KS,
        )
        .await
        .expect("insert into a named graph");

    let expected: HashSet<(String, String)> = [
        pair("a", "p"),
        pair("b", "p"),
        pair("c", "p"),
        pair("d", "q"),
    ]
    .into_iter()
    .collect();

    let before = served_object_rows(&f.engine, "shared").await;
    f.storage.flush_all().expect("flush to sstables");
    let after = served_object_rows(&f.engine, "shared").await;

    assert_eq!(
        before, expected,
        "an object-bound query must return exactly the triples whose object matches"
    );
    assert_eq!(
        after, expected,
        "the same triples must come back once they have been flushed to SSTables"
    );
    assert!(
        !before.contains(&pair("z", "p")),
        "a triple in a named graph must not appear in a scope that isolates the \
         default graph, even when the index that answered is keyed on the object alone"
    );

    assert!(
        f.storage
            .declares_index(&triples_table_id(KS), OBJECT_INDEX),
        "'{OBJECT_INDEX}' is not declared for rdf_triples in keyspace '{KS}', so the \
         object-bound access path streams a full table scan instead of reading the \
         index the executor's ObjectScan arm is written against"
    );
}

/// The served shape: the caller names keyspace `rdf` while the config's
/// `default_graph` stays `default`, so the keyspace's table is registered
/// LAZILY on the first statement rather than in `new`.
///
/// RED as written: fails on the lazy site's `declares_index` assertion, which is
/// a different registration site from the one above.
#[tokio::test]
async fn a_lazily_registered_keyspace_also_gets_the_object_index() {
    let f = fixture(SparqlConfig::default());
    assert!(
        f.storage
            .declares_index(&triples_table_id("default"), OBJECT_INDEX),
        "the default graph's keyspace must carry the index from `new`"
    );

    f.engine
        .execute_update(
            "INSERT DATA { <urn:qa:probe> <urn:qa:says> \"hello\" }",
            "rdf",
        )
        .await
        .expect("insert data");

    assert!(
        f.storage
            .declares_index(&triples_table_id("rdf"), OBJECT_INDEX),
        "keyspace 'rdf' is registered on first use, and registration must declare \
         '{OBJECT_INDEX}' there too"
    );
}

// =======================================================================
// 2 — the query stops reading once the index has answered.
// =======================================================================

/// Over a 20-row table under an 8-row bound, an object-bound query can only
/// complete by reading the index: the fallback streams every partition.
///
/// RED as written: the scan reads all 20 rows and dies on the executor's own
/// row bound (`SPARQL scan read more than 8 storage rows ...`). The control in
/// the same test lifts the bound and requires the same single row back, so the
/// failure is about work done and not about the query being unanswerable.
#[tokio::test]
async fn an_object_bound_query_is_answered_without_reading_the_whole_table() {
    let f = fixture(SparqlConfig {
        default_graph: KS.to_string(),
        ..Default::default()
    });
    f.seed().await;

    // The control this test stands for: the index it reads through is DECLARED
    // and CURRENT. Without that pin, a change that sent every object-bound read
    // back to the scan would still look answered here — the row is found either
    // way — and only the bound below would notice.
    assert!(
        f.storage
            .declares_index(&triples_table_id(KS), OBJECT_INDEX)
            && f.storage
                .index_is_current(&triples_table_id(KS), OBJECT_INDEX),
        "this test is about the index read; it means nothing unless the fixture's \
         '{OBJECT_INDEX}' is both declared and current"
    );

    let indexed = f
        .run_bounded("SELECT ?s ?p ?o WHERE { ?s ?p \"object-019\" }", BOUND)
        .await
        .expect(
            "a 20-row table under an 8-row bound is answerable only by the index; a full \
             scan reads every row and refuses to truncate",
        );
    assert_eq!(
        indexed,
        vec![pair("s19", "p")],
        "the indexed read must return the one triple whose object is 'object-019'"
    );

    let unbounded = f
        .run_bounded("SELECT ?s ?p ?o WHERE { ?s ?p \"object-019\" }", TABLE_ROWS)
        .await
        .expect("the same query fits inside the full-table bound");
    assert_eq!(
        unbounded, indexed,
        "control: with the whole table's worth of bound the query must answer identically, \
         so the failure at the tight bound is about how much was read"
    );

    let whole_table = f
        .run_bounded("SELECT ?s ?p ?o WHERE { ?s ?p \"absent\" }", BOUND)
        .await
        .expect_err(
            "control: an object nothing matches has no posting, so that read still falls \
             back to a scan and must trip the same bound",
        );
    assert!(
        whole_table
            .to_string()
            .contains("read more than 8 storage rows"),
        "the control must die on the row bound, not on something else: {whole_table}"
    );
}

// =======================================================================
// 3 — MIRROR CONTROL: the fallback is still there.
// =======================================================================

/// A table with NO declared object index must still full-scan, must still
/// answer with the right values, and must still say that it is scanning.
///
/// The declaration is dropped after seeding, which leaves exactly the table the
/// shipped code produced. This deliberately does NOT go back through
/// `SparqlEngine::execute`: registration is the single place that declares the
/// index, so the served entry point would re-create the very index this control
/// removed.
#[tokio::test]
async fn an_undeclared_object_index_still_scans_and_still_warns() {
    let f = fixture(SparqlConfig {
        default_graph: KS.to_string(),
        ..Default::default()
    });
    f.seed().await;

    let removed = f
        .storage
        .drop_index(&triples_table_id(KS), OBJECT_INDEX)
        .expect("drop the declared index");
    assert!(
        removed,
        "the registration above must have declared something to drop"
    );
    assert!(
        !f.storage
            .declares_index(&triples_table_id(KS), OBJECT_INDEX),
        "control: the index must be undeclared for this test to mean anything"
    );

    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    // The values are still right — an absent index is a slower answer, not a
    // wrong one or a refusal.
    let scanned = f
        .run_bounded("SELECT ?s ?p ?o WHERE { ?s ?p \"object-019\" }", TABLE_ROWS)
        .await
        .expect("an undeclared index must fall back to the scan, not refuse");
    assert_eq!(
        scanned,
        vec![pair("s19", "p")],
        "the fallback scan must decode the same triple the index would have found"
    );

    // ...and it really did read the table, rather than finding the row through
    // an index it does not declare.
    let tripped = f
        .run_bounded("SELECT ?s ?p ?o WHERE { ?s ?p \"object-019\" }", BOUND)
        .await
        .expect_err(
            "control: with no declared index the read must still stream the whole table, so \
             the 8-row bound must trip exactly as it did before the index existed",
        );
    assert!(
        tripped
            .to_string()
            .contains("read more than 8 storage rows"),
        "the fallback must die on the row bound, not on a refused index read: {tripped}"
    );

    let captured = logs.contents();
    assert!(
        captured.contains("ObjectScan: object index is not declared"),
        "the fallback must still warn that it is streaming a full scan; captured: {captured}"
    );
    assert!(
        captured.contains(OBJECT_INDEX),
        "the warning must name the index that is missing; captured: {captured}"
    );
}

/// Every SSTable generation in `table_dir`, by the id the tracker and the build
/// jobs name it by. `<gen>-Data.db` is how a generation is discovered on disk
/// (see `StorageEngine::list_generations_in_dir`), and the test reads it here
/// rather than asking the engine, so a generation the engine has stopped
/// tracking cannot make an assertion about "the pre-existing data" vacuous.
fn generations_in(table_dir: &std::path::Path) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(table_dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .strip_suffix("-Data.db")
                .map(std::string::ToString::to_string)
        })
        .collect();
    out.sort();
    out
}

/// Wait (bounded) for the object index to reach `is_current`.
///
/// The bound is a hang guard, not a latency assertion: the backfill of one
/// 20-row SSTable finishes in milliseconds, and what is under test is the
/// STATE the backfill reaches, never how fast it got there.
fn wait_for_object_index_current(storage: &StorageEngine) -> bool {
    let table_id = triples_table_id(KS);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !storage.index_is_current(&table_id, OBJECT_INDEX) && std::time::Instant::now() < deadline
    {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    storage.index_is_current(&table_id, OBJECT_INDEX)
}

/// [`wait_for_object_index_current`] for the opposite outcome: the object
/// index's build failed and the failure is recorded.
fn wait_for_object_index_failure(storage: &StorageEngine) -> bool {
    let failed = || {
        storage
            .stale_secondary_indexes()
            .iter()
            .any(|stale| stale.index == OBJECT_INDEX && stale.last_error.is_some())
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !failed() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    failed()
}

// =======================================================================
// 5 — CONVERGENCE: a declared index is driven to current with no operator.
// =======================================================================

/// A node that comes up over triples ALREADY on disk declares the object index
/// over them and the backfill it queued reaches `is_current` with no operator
/// action — after which the object-bound query reads through the repaired index
/// rather than scanning.
///
/// This is the third creation path (the other two are `new` and the lazy
/// registration, pinned above): the rows reach an SSTable while the index is
/// NOT declared, which is the shape of a node whose data predates the index.
/// The declaration submits a build for every generation that predates it; the
/// backend is the real one (`IndexBackendConfig::Local`), so the only thing the
/// test supplies is the wait.
///
/// RED as written against the reverted declaration (588d1bc7^): nothing declares
/// the index, so there is no backfill to converge and the tight-bound read below
/// streams all 20 rows and refuses to truncate.
#[tokio::test]
async fn a_node_booting_over_existing_triples_drives_the_object_index_to_current() {
    let f = fixture(SparqlConfig {
        default_graph: KS.to_string(),
        ..Default::default()
    });
    f.seed().await;

    // The rows reach an SSTable while the index is not declared, so no sidecar
    // is written for them: `flush_all` builds sidecars only for the indexes the
    // table maintains.
    assert!(
        f.storage
            .drop_index(&triples_table_id(KS), OBJECT_INDEX)
            .expect("drop the declared index"),
        "the fixture's registration must have declared '{OBJECT_INDEX}' to drop"
    );
    f.storage
        .flush_all()
        .expect("the seeded rows must reach an SSTable");
    assert!(
        !f.storage
            .declares_index(&triples_table_id(KS), OBJECT_INDEX),
        "the fixture is only meaningful while the index is genuinely absent"
    );
    let table_dir = f.storage.table_sstable_dir(&triples_table_id(KS));
    assert_eq!(
        generations_in(&table_dir).len(),
        1,
        "the seeded rows must be in exactly one pre-existing generation"
    );

    // Boot: registration over pre-existing SSTables.
    let engine = SparqlEngine::new(
        Arc::clone(&f.storage),
        Arc::clone(&f.write_path),
        f.config.clone(),
    );
    assert!(
        f.storage
            .declares_index(&triples_table_id(KS), OBJECT_INDEX),
        "registration over pre-existing SSTables must declare '{OBJECT_INDEX}'"
    );

    // Convergence, with the real scheduler and no operator action.
    assert!(
        wait_for_object_index_current(&f.storage),
        "a declared index must reach current on its own; still not current: {:?}",
        f.storage.stale_secondary_indexes()
    );
    assert!(
        f.storage.stale_secondary_indexes().is_empty(),
        "nothing may be left reported as degraded once the backfill has caught up: {:?}",
        f.storage.stale_secondary_indexes()
    );

    // The repair is REAL, not merely a status: over a 20-row table under an
    // 8-row bound, only an index read can answer. The scan counts every storage
    // row at the source and refuses to truncate.
    let indexed = f
        .run_bounded("SELECT ?s ?p ?o WHERE { ?s ?p \"object-019\" }", BOUND)
        .await
        .expect(
            "the repaired index must answer a 20-row table under an 8-row bound; a scan \
             reads every row and refuses to truncate",
        );
    assert_eq!(
        indexed,
        vec![pair("s19", "p")],
        "the repairable index must return exactly the triple stored under 'object-019'"
    );

    // The row the query above did not ask for is still reachable, so the index
    // the repair produced is complete rather than narrow.
    let other = served_object_rows(&engine, "object-004").await;
    assert_eq!(
        other,
        [pair("s4", "p")].into_iter().collect(),
        "every pre-existing triple must be in the index the boot repaired"
    );
}

/// An index build interrupted before it finished leaves a half-written sidecar
/// on disk. With no builder available it must stay VISIBLY not current — the
/// artifact must never be read as a completed build — and the read path must
/// answer correctly by scanning meanwhile.
///
/// The artifact is real: the writer's own 17-byte header, with no body and no
/// footer, which is exactly what a build killed mid-write leaves behind. The
/// backend is `Off` so the window is held still rather than raced: nothing
/// drains the pending set, and what the tracker says about the generation is
/// the same either way.
#[tokio::test]
async fn an_interrupted_object_index_build_is_not_current_and_the_read_still_scans() {
    let f = fixture_with_index_backend(
        SparqlConfig {
            default_graph: KS.to_string(),
            ..Default::default()
        },
        IndexBackendConfig::Off,
    );
    f.seed().await;
    f.storage
        .flush_all()
        .expect("the seeded rows must reach an SSTable");
    let table_dir = f.storage.table_sstable_dir(&triples_table_id(KS));
    let generations = generations_in(&table_dir);
    assert_eq!(generations.len(), 1, "one flushed generation");

    // The build that never finished: a header, no body, no footer.
    let sidecar = table_dir.join(format!("{}-{OBJECT_INDEX}.sidecar", generations[0]));
    let mut partial = b"FXSI".to_vec();
    partial.push(2);
    partial.extend_from_slice(&1u64.to_le_bytes());
    partial.extend_from_slice(&[0u8; 4]);
    partial.extend_from_slice(b"\x07\x00\x00\x00shared");
    std::fs::write(&sidecar, &partial).expect("write the half-written sidecar");
    assert!(
        sidecar.exists(),
        "the artifact must be on disk, or the test proves nothing about trusting it"
    );

    // The next boot declares the index and finds the generation unbuilt.
    assert!(f
        .storage
        .drop_index(&triples_table_id(KS), OBJECT_INDEX)
        .expect("drop the declared index"));
    let engine = SparqlEngine::new(
        Arc::clone(&f.storage),
        Arc::clone(&f.write_path),
        f.config.clone(),
    );

    assert!(
        f.storage
            .declares_index(&triples_table_id(KS), OBJECT_INDEX),
        "the boot must declare '{OBJECT_INDEX}' over the generation it found"
    );
    assert!(
        !f.storage
            .index_is_current(&triples_table_id(KS), OBJECT_INDEX),
        "a half-written sidecar must not be read as a completed build: {:?}",
        f.storage.stale_secondary_indexes()
    );

    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    // The rows come back — by scanning, since the index cannot be trusted to
    // answer for this generation.
    assert_eq!(
        served_object_rows_or_error(&engine, "object-019").await,
        Ok([pair("s19", "p")].into_iter().collect()),
        "while the interrupted build is unfinished the scan must still answer with the stored row"
    );
    let tripped = f
        .run_bounded("SELECT ?s ?p ?o WHERE { ?s ?p \"object-019\" }", BOUND)
        .await
        .expect_err(
            "the read must stream the whole table rather than answer out of a sidecar whose \
             build never finished",
        );
    assert!(
        tripped
            .to_string()
            .contains("read more than 8 storage rows"),
        "the fallback must die on the row bound, not on a refused index read: {tripped}"
    );
    let captured = logs.contents();
    assert!(
        captured.contains("not yet current") && captured.contains(OBJECT_INDEX),
        "the scan fallback must warn, naming the index it is standing in for; \
         captured: {captured}"
    );
}

/// The same half-written sidecar over a node that DOES have a builder: the boot
/// re-drives the build rather than trusting the artifact, the index reaches
/// current, and the object-bound query then reads through it.
///
/// The pair matters. This test alone would pass if the artifact were trusted
/// (the query would be answered either way); the case above alone would pass if
/// the artifact were trusted and never repaired. Together they say the artifact
/// is neither trusted NOR fatal.
#[tokio::test]
async fn an_interrupted_object_index_build_is_redone_and_the_index_reaches_current() {
    let f = fixture(SparqlConfig {
        default_graph: KS.to_string(),
        ..Default::default()
    });
    f.seed().await;
    assert!(
        f.storage
            .drop_index(&triples_table_id(KS), OBJECT_INDEX)
            .expect("drop the declared index"),
        "the fixture's registration must have declared '{OBJECT_INDEX}' to drop"
    );
    f.storage
        .flush_all()
        .expect("the seeded rows must reach an SSTable");

    // A build that died mid-write: the sidecar's header landed, its body did
    // not.
    let table_dir = f.storage.table_sstable_dir(&triples_table_id(KS));
    let generations = generations_in(&table_dir);
    assert_eq!(generations.len(), 1, "one flushed generation");
    let partial = table_dir.join(format!("{}-{OBJECT_INDEX}.sidecar", generations[0]));
    let mut artifact = b"FXSI".to_vec();
    artifact.push(2);
    artifact.extend_from_slice(&1u64.to_le_bytes());
    artifact.extend_from_slice(&[0u8; 4]);
    artifact.extend_from_slice(b"\x07\x00\x00\x00shared");
    std::fs::write(&partial, &artifact).expect("write the half-written sidecar");

    let _engine = SparqlEngine::new(
        Arc::clone(&f.storage),
        Arc::clone(&f.write_path),
        f.config.clone(),
    );
    assert!(
        f.storage
            .declares_index(&triples_table_id(KS), OBJECT_INDEX),
        "the boot must declare '{OBJECT_INDEX}' over the generation it found"
    );

    assert!(
        wait_for_object_index_current(&f.storage),
        "the build the interruption cut short must be re-driven until it completes: {:?}",
        f.storage.stale_secondary_indexes()
    );
    assert!(
        f.storage.stale_secondary_indexes().is_empty(),
        "nothing may be left reported as degraded once the build has completed: {:?}",
        f.storage.stale_secondary_indexes()
    );
    let indexed = f
        .run_bounded("SELECT ?s ?p ?o WHERE { ?s ?p \"object-019\" }", BOUND)
        .await
        .expect("the completed index must answer without reading the whole table");
    assert_eq!(indexed, vec![pair("s19", "p")]);
}

// =======================================================================
// 6 — NO SILENT DEGRADATION: a build that keeps failing is OBSERVABLE.
// =======================================================================

/// A build that fails must be reported where an operator looks — the index's
/// status, its error, its gauge, and a log line naming the index AND the
/// generation — and the read path must keep answering correctly by scanning
/// for as long as the index is not current.
///
/// The failure is injected where the real one happens: the build reads the
/// SSTable successfully and cannot publish its sidecar, because the path the
/// sidecar goes to is occupied by a directory. So the DATA stays readable and
/// the two halves of this contract can be asserted together — the failure being
/// reported must not be confused with the data being unavailable.
#[tokio::test]
async fn a_failing_object_index_build_is_reported_by_name_and_the_read_keeps_scanning() {
    let f = fixture(SparqlConfig {
        default_graph: KS.to_string(),
        ..Default::default()
    });
    f.seed().await;
    assert!(
        f.storage
            .drop_index(&triples_table_id(KS), OBJECT_INDEX)
            .expect("drop the declared index"),
        "the fixture's registration must have declared '{OBJECT_INDEX}' to drop"
    );
    f.storage
        .flush_all()
        .expect("the seeded rows must reach an SSTable");

    let table_dir = f.storage.table_sstable_dir(&triples_table_id(KS));
    let generations = generations_in(&table_dir);
    assert_eq!(generations.len(), 1, "one flushed generation");
    let generation = generations[0].clone();
    let blocked = table_dir.join(format!("{generation}-{OBJECT_INDEX}.sidecar"));
    std::fs::create_dir(&blocked).expect("occupy the sidecar path with a directory");

    // The build runs on a scheduler worker, not on this thread, so the log the
    // failure is reported in has to be a GLOBAL subscriber to be captured at
    // all. Nothing else in this binary installs one.
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("this binary installs the global tracing subscriber exactly once");

    let f = Fixture {
        engine: SparqlEngine::new(
            Arc::clone(&f.storage),
            Arc::clone(&f.write_path),
            f.config.clone(),
        ),
        ..f
    };
    assert!(
        f.storage
            .declares_index(&triples_table_id(KS), OBJECT_INDEX),
        "the boot must declare '{OBJECT_INDEX}' over the generation it found"
    );
    assert!(
        wait_for_object_index_failure(&f.storage),
        "a failed backfill build must be recorded, not swallowed: {:?}",
        f.storage.stale_secondary_indexes()
    );

    // STATUS: the index, its table, the outstanding generation, and the reason.
    let stale = f.storage.publish_index_backfill_status();
    assert_eq!(stale.len(), 1, "{stale:?}");
    assert_eq!(
        (stale[0].table.clone(), stale[0].index.as_str()),
        (triples_table_id(KS), OBJECT_INDEX),
        "the reported index must be the graph's object index, named: {stale:?}"
    );
    assert_eq!(stale[0].pending_generations, 1, "{stale:?}");
    let error = stale[0]
        .last_error
        .as_deref()
        .expect("a failed backfill must report why it failed");
    assert!(
        error.contains(&generation),
        "the error must name the generation whose build failed: {error}"
    );
    assert!(
        !error.contains("not current"),
        "the reported failure must be the build's error, not a refusal at the read: {error}"
    );

    // COUNTER: the gauges an operator scrapes, labelled with the index.
    let table = triples_table_id(KS).to_string();
    assert_eq!(
        ferrosa_storage::metrics::index_not_current(&table, OBJECT_INDEX),
        1,
        "a not-current index must be counted"
    );
    assert!(
        ferrosa_storage::metrics::index_backfill_failed(&table, OBJECT_INDEX),
        "a failed backfill must be flagged"
    );

    // LOG: the line names both the index and the generation that failed.
    let captured = logs.contents();
    let failure_line = captured
        .lines()
        .find(|line| line.contains("index backfill FAILED"))
        .unwrap_or_else(|| panic!("no failure was logged; captured: {captured}"));
    assert!(
        failure_line.contains(OBJECT_INDEX) && failure_line.contains(&generation),
        "the failure must name the index and the sstable: {failure_line}"
    );

    // READ: still correct, still by scanning, and still saying so.
    assert_eq!(
        served_object_rows_or_error(&f.engine, "object-019").await,
        Ok([pair("s19", "p")].into_iter().collect()),
        "a failing index must not cost the caller a correct answer"
    );
    let tripped = f
        .run_bounded("SELECT ?s ?p ?o WHERE { ?s ?p \"object-019\" }", BOUND)
        .await
        .expect_err(
            "with the index not current the read must stream the whole table, so the \
             8-row bound must trip — an answer at that bound would come from an \
             incomplete index",
        );
    assert!(
        tripped
            .to_string()
            .contains("read more than 8 storage rows"),
        "the fallback must die on the row bound, not on a refused index read: {tripped}"
    );
    assert!(
        logs.contents().contains(OBJECT_INDEX) && logs.contents().contains("not yet current"),
        "the scan fallback must warn, naming the index it is standing in for; \
         captured: {}",
        logs.contents()
    );

    std::fs::remove_dir(&blocked).expect("clear the blocked sidecar path");
}

// =======================================================================
// 4 — the backfill window: declared, but not yet current.
// =======================================================================

/// An object-bound query must fall back to the full scan while the index it
/// declares is still backfilling — the right rows, slow, and loud about it.
///
/// `read_by_index_each_after` REFUSES a read through an index whose backfill
/// has not finished: the index holds a fraction of the table, and a fraction
/// reported as the whole answer is worse than a slow answer. So the access
/// path has to be chosen on `declared AND current`, not on `declared` — in the
/// window between the declaration and the completed build, the index path
/// answers with an error where the scan underneath it answers correctly.
///
/// RED as written (under the declaration-alone predicate): the served query
/// returns `Err("secondary index 'rdf_triples_object_idx' on rdf_triples in
/// keyspace 'default' is not current; refusing to return incomplete results
/// while index backfill is pending or failed")` in place of the row that is on
/// disk and that the fallback scan finds.
#[tokio::test]
async fn an_object_bound_query_scans_while_the_declared_index_is_not_yet_current() {
    let f = fixture_with_index_backend(
        SparqlConfig {
            default_graph: KS.to_string(),
            ..Default::default()
        },
        IndexBackendConfig::Off,
    );
    f.seed().await;
    let engine = f.leave_object_index_declared_but_not_current();

    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    // The contract: the rows come back. The declared index is incomplete, the
    // table is not, and the scan is the access path that reads the table.
    let expected: HashSet<(String, String)> = [pair("s19", "p")].into_iter().collect();
    let rows = served_object_rows_or_error(&engine, "object-019").await;
    assert_eq!(
        rows,
        Ok(expected),
        "an index whose backfill is still pending must not be consulted: falling back to the \
         scan returns the one triple whose object is 'object-019', and the index path does not"
    );

    // ...and it really scanned, rather than finding the row through the index
    // it declares. The 8-row bound is answerable only out of index postings, so
    // a read that dies on it streamed the table.
    let tripped = f
        .run_bounded("SELECT ?s ?p ?o WHERE { ?s ?p \"object-019\" }", BOUND)
        .await
        .expect_err(
            "while the index is not current it cannot answer, so the read must stream the \
             whole table and trip the 8-row bound",
        );
    assert!(
        tripped
            .to_string()
            .contains("read more than 8 storage rows"),
        "the fallback must die on the row bound, not on a refused index read: {tripped}"
    );

    // A fallback nobody can see is a fallback nobody can explain in an
    // incident, and the window it is covering has to be named.
    let captured = logs.contents();
    assert!(
        captured.contains("ObjectScan") && captured.contains(OBJECT_INDEX),
        "the scan fallback must warn, and the warning must name the index it is standing in \
         for; captured: {captured}"
    );
}
