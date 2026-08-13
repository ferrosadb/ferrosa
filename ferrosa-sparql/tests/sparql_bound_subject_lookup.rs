//! A bound-subject triple pattern must return the triples it stores.
//!
//! Found from outside, against a running install: writing a triple and then
//! asking for it by subject returned nothing, while the same triple came back
//! from an unbound scan and from a bound-predicate pattern.
//!
//!     INSERT DATA { <urn:qa:probe> <urn:qa:says> "hello" }   -> inserted 1
//!     SELECT ?s WHERE { ?s ?p ?o }                           -> the triple
//!     SELECT ?s WHERE { ?s <urn:qa:says> ?o }                -> the triple
//!     SELECT ?o WHERE { <urn:qa:probe> <urn:qa:says> ?o }    -> EMPTY
//!
//! A bound-subject lookup is the commonest SPARQL shape ("tell me about this
//! thing"). It fails silently and returns a well-formed empty result set, so
//! nothing upstream can tell it apart from "that subject has no triples" -- a
//! consumer verifying a write this way concludes its data was lost.
//!
//! These tests pin the three access paths against each other. The comparison is
//! what makes them useful: any one of them alone could be argued to be an empty
//! database, and only disagreement between them proves a read path is wrong.

use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_sparql::engine::{SparqlConfig, SparqlEngine, SparqlResult};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use tempfile::TempDir;

// "rdf" -- the keyspace the HTTP surface actually defaults to, and the value
// that made every one of these fail before the keyspace/graph split.
//
// Using "default" here would hide the bug entirely: with keyspace == graph the
// reader and writer happen to agree, which is exactly why the neighbouring
// pattern-delete suite (which pins "default" and says the coupling otherwise
// "gets in the way") never caught it. Testing the value real callers use is the
// whole point.
const KS: &str = "rdf";

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
    // The DEFAULT config, deliberately: default_graph stays "default" while the
    // keyspace is "rdf". That is the production shape -- SparqlConfig::default()
    // sets "default", and http.rs defaults the keyspace to "rdf".
    //
    // Tying default_graph to the keyspace (as the neighbouring suites do) makes
    // reader and writer agree by accident and hides the very bug this file
    // exists for.
    SparqlEngine::new(storage, write_path, SparqlConfig::default())
}

async fn select_count(eng: &SparqlEngine, query: &str) -> usize {
    match eng.execute(query, KS).await.expect("select must succeed") {
        SparqlResult::Select(r) => r.results.bindings.len(),
        SparqlResult::Ask(_) => panic!("expected SELECT result, got ASK"),
        SparqlResult::Graph(_) => panic!("expected SELECT result, got Graph"),
    }
}

/// The same three paths, after the data has been FLUSHED to SSTables.
///
/// This is the case the in-memory cases cannot reach, and the one a running
/// install is always in: everything written before the last flush is read back
/// through the SSTable path rather than the memtable. A point read that works
/// against a memtable and misses against an SSTable would look exactly like
/// what was seen from outside -- the scan still finds the triple because it
/// reads every partition, while the bound-subject lookup asks for one key and
/// is told there is nothing there.
#[tokio::test]
async fn every_access_path_finds_a_flushed_triple() {
    let (storage, wp, _dir) = setup();
    let eng = engine(Arc::clone(&storage), wp);

    eng.execute_update("INSERT DATA { <urn:qa:probe> <urn:qa:says> \"hello\" }", KS)
        .await
        .expect("insert data");

    // Force the write out of the memtable before reading it back.
    storage.flush_all().expect("flush to sstables");

    let unbound = select_count(&eng, "SELECT ?s WHERE { ?s ?p ?o }").await;
    let bound_predicate = select_count(&eng, "SELECT ?s WHERE { ?s <urn:qa:says> ?o }").await;
    let bound_subject =
        select_count(&eng, "SELECT ?o WHERE { <urn:qa:probe> <urn:qa:says> ?o }").await;

    assert_eq!(
        (unbound, bound_predicate, bound_subject),
        (1, 1, 1),
        "after a flush the three access paths disagree about a stored triple \
(unbound={unbound}, bound predicate={bound_predicate}, bound subject={bound_subject}). \
A path returning 0 while another returns 1 is a wrong answer, not an empty database."
    );
}

/// The three access paths must agree about a triple that exists.
///
/// Written as one test rather than three so a failure reports the DISAGREEMENT,
/// which is the finding. Three separate tests would report "bound subject
/// returned 0", which reads like an empty database.
#[tokio::test]
async fn every_access_path_finds_a_triple_that_was_just_written() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update("INSERT DATA { <urn:qa:probe> <urn:qa:says> \"hello\" }", KS)
        .await
        .expect("insert data");

    let unbound = select_count(&eng, "SELECT ?s WHERE { ?s ?p ?o }").await;
    let bound_predicate = select_count(&eng, "SELECT ?s WHERE { ?s <urn:qa:says> ?o }").await;
    let bound_subject =
        select_count(&eng, "SELECT ?o WHERE { <urn:qa:probe> <urn:qa:says> ?o }").await;

    assert_eq!(
        (unbound, bound_predicate, bound_subject),
        (1, 1, 1),
        "the three access paths disagree about a triple that was just written \
(unbound={unbound}, bound predicate={bound_predicate}, bound subject={bound_subject}). \
A path returning 0 while another returns 1 is a wrong answer, not an empty database."
    );
}

/// The same, with the subject bound and the predicate left free -- so a failure
/// separates "binding the subject breaks it" from "binding two terms breaks it".
#[tokio::test]
async fn a_bound_subject_alone_finds_its_triples() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update(
        "INSERT DATA { \
            <urn:qa:probe> <urn:qa:says> \"hello\" . \
            <urn:qa:probe> <urn:qa:also> \"world\" . \
            <urn:qa:other> <urn:qa:says> \"elsewhere\" }",
        KS,
    )
    .await
    .expect("insert data");

    let bound_subject = select_count(&eng, "SELECT ?p ?o WHERE { <urn:qa:probe> ?p ?o }").await;
    assert_eq!(
        bound_subject, 2,
        "a bound subject must return exactly its own triples, not zero and not the whole store"
    );
}

/// A subject that genuinely has no triples must return zero -- otherwise a fix
/// for the above could be "return everything", which would pass the tests above
/// while being just as wrong in the other direction.
#[tokio::test]
async fn a_bound_subject_with_no_triples_returns_nothing() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update("INSERT DATA { <urn:qa:probe> <urn:qa:says> \"hello\" }", KS)
        .await
        .expect("insert data");

    let absent = select_count(&eng, "SELECT ?o WHERE { <urn:qa:absent> <urn:qa:says> ?o }").await;
    assert_eq!(absent, 0, "a subject with no triples must return nothing");
}
