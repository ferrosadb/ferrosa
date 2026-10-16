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
    _dir: TempDir,
}

/// `keyspace == default_graph`: rows land in the graph component the served
/// scope isolates on, so an index that ignored the graph would be visible here.
fn fixture(config: SparqlConfig) -> Fixture {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(StorageEngine::new(storage_config(&dir), None).unwrap());
    let write_path = Arc::new(WritePath::direct(Arc::clone(&storage)));
    let engine = SparqlEngine::new(Arc::clone(&storage), Arc::clone(&write_path), config);
    Fixture {
        storage,
        write_path,
        engine,
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
}

/// `(subject, predicate)` of every solution, sorted. Every query in this file
/// binds its object as a pattern constant, so these two are the whole row.
fn rows_of(bindings: &[std::collections::HashMap<String, Binding>]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = bindings
        .iter()
        .map(|row| (row["s"].value.clone(), row["p"].value.clone()))
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
            .map(|row| (row["s"].value.clone(), row["p"].value.clone()))
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
