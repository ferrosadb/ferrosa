//! M3 completeness tests — URS-QEC-S01/S02/S03/S04.
//!
//! RED tests (written before implementation); each must fail until the
//! corresponding implementation is in place.  Every test documents its
//! requirement ID and fail-loud expectation.
//!
//! ## Summary of requirements
//!
//! | ID | What | Fail-loud rule |
//! |---|---|---|
//! | URS-QEC-S01 | CONSTRUCT / DESCRIBE query forms | `plan_query` must return Ok for these forms; engine must produce graph output |
//! | URS-QEC-S02 | Full SPARQL UPDATE beyond M1: INSERT WHERE | INSERT WHERE must persist triples |
//! | URS-QEC-S03 | RDF* eval (silent → fail loud) + SPARQL XML results | annotated var must NOT silently be absent; XML serialization round-trip |
//! | URS-QEC-S04 | ORDER BY expressions (silent → fail loud) | non-variable ORDER BY must return Err, not silently skip |

use std::collections::HashMap;
use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_sparql::engine::{SparqlConfig, SparqlEngine, SparqlResult};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use tempfile::TempDir;

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

async fn select_count(eng: &SparqlEngine, query: &str) -> usize {
    match eng.execute(query, KS).await.expect("select must succeed") {
        SparqlResult::Select(r) => r.results.bindings.len(),
        SparqlResult::Ask(_) => panic!("expected SELECT result, got ASK"),
        SparqlResult::Graph(_) => panic!("expected SELECT result, got Graph"),
    }
}

async fn select_values(eng: &SparqlEngine, query: &str, var: &str) -> Vec<String> {
    match eng.execute(query, KS).await.expect("select must succeed") {
        SparqlResult::Select(r) => r
            .results
            .bindings
            .iter()
            .filter_map(|row| row.get(var).map(|b| b.value.clone()))
            .collect(),
        SparqlResult::Ask(_) => panic!("expected SELECT, got ASK"),
        SparqlResult::Graph(_) => panic!("expected SELECT, got Graph"),
    }
}

// ---------------------------------------------------------------------------
// URS-QEC-S01 — CONSTRUCT and DESCRIBE query forms
// ---------------------------------------------------------------------------

/// S01-a: CONSTRUCT query must succeed and produce graph output (not a Plan error).
/// The result must serialize without error; the constructed triples must be
/// present in the serialized graph.
#[tokio::test]
async fn construct_query_produces_graph_result() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    // Seed some data.
    eng.execute_update(
        "INSERT DATA { \
            <http://ex/alice> <http://ex/name> \"Alice\" . \
            <http://ex/bob>   <http://ex/name> \"Bob\" }",
        KS,
    )
    .await
    .expect("insert data");

    // CONSTRUCT copies matched triples into a new graph template.
    let result = eng
        .execute(
            "CONSTRUCT { ?s <http://ex/name> ?name } \
             WHERE { ?s <http://ex/name> ?name }",
            KS,
        )
        .await
        .expect("CONSTRUCT must not return a plan error");

    // The result must be a Graph variant (not Select/Ask).
    match &result {
        SparqlResult::Graph(triples) => {
            assert_eq!(triples.len(), 2, "CONSTRUCT must produce 2 triples");
        }
        other => panic!("expected SparqlResult::Graph, got {other:?}"),
    }

    // Must serialize to N-Triples without error.
    let nt = String::from_utf8(result.to_ntriples()).unwrap();
    assert!(
        nt.contains("Alice") || nt.contains("alice"),
        "serialized CONSTRUCT result must contain Alice: {nt}"
    );
}

/// S01-b: DESCRIBE query must succeed and produce graph output for the
/// described resource.
#[tokio::test]
async fn describe_query_produces_graph_result() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update(
        "INSERT DATA { \
            <http://ex/alice> <http://ex/name>  \"Alice\" . \
            <http://ex/alice> <http://ex/email> \"alice@ex.com\" }",
        KS,
    )
    .await
    .expect("insert data");

    let result = eng
        .execute("DESCRIBE <http://ex/alice>", KS)
        .await
        .expect("DESCRIBE must not return a plan error");

    match &result {
        SparqlResult::Graph(triples) => {
            assert_eq!(
                triples.len(),
                2,
                "DESCRIBE must return both triples about :alice"
            );
        }
        other => panic!("expected SparqlResult::Graph for DESCRIBE, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// URS-QEC-S02 — Full SPARQL UPDATE: INSERT … WHERE
// ---------------------------------------------------------------------------

/// S02: INSERT … WHERE must evaluate the WHERE clause, instantiate the INSERT
/// template per solution, and persist the resulting triples.  After the
/// operation a SELECT must return the newly inserted data.
#[tokio::test]
async fn insert_where_persists_new_triples() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    // Seed: alice is a Person.
    eng.execute_update(
        "INSERT DATA { <http://ex/alice> <http://ex/type> <http://ex/Person> }",
        KS,
    )
    .await
    .expect("insert data");

    // INSERT WHERE: for every Person, assert they are also a LegalEntity.
    let res = eng
        .execute_update(
            "INSERT { ?s <http://ex/type> <http://ex/LegalEntity> } \
             WHERE  { ?s <http://ex/type> <http://ex/Person> }",
            KS,
        )
        .await
        .expect("INSERT WHERE must succeed");

    assert_eq!(res.triples_inserted, 1, "one triple must be inserted");

    // The new triple must now be visible to SELECT.
    let types = select_values(
        &eng,
        "SELECT ?t WHERE { <http://ex/alice> <http://ex/type> ?t }",
        "t",
    )
    .await;
    assert!(
        types.iter().any(|t| t.contains("LegalEntity")),
        "LegalEntity triple must be visible after INSERT WHERE; got: {types:?}"
    );
}

// ---------------------------------------------------------------------------
// URS-QEC-S03a — RDF* must fail loud (not return inner bindings silently)
// ---------------------------------------------------------------------------

/// S03-a (URS-QEC-X01): A SPARQL query using RDF* annotation syntax must
/// return a clear SPARQL protocol error — NOT return inner bindings as if the
/// annotation variable is simply absent.
///
/// Current (broken) behaviour: `evaluate_rdf_star_pattern` logs a warning and
/// returns the inner bindings with the annotation variable unbound.  That is a
/// silent wrong result — the caller gets rows that look valid but are missing
/// the requested annotation data.
///
/// Required behaviour: the engine returns `Err(SparqlError::Plan(…))` so the
/// HTTP layer returns 400 Bad Request, not a silently incomplete 200.
#[tokio::test]
async fn rdf_star_annotation_fails_loud_not_silent_wrong_result() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    // We don't need stored data — the query itself must fail loud before
    // hitting storage.
    let err = eng
        .execute(
            "SELECT ?conf WHERE { \
                << <http://ex/a> <http://ex/link> <http://ex/b> >> \
                   <http://ex/confidence> ?conf }",
            KS,
        )
        .await
        .expect_err(
            "RDF* annotation query must return Err (fail loud), \
             not Ok with the annotation variable silently absent",
        );

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("rdf*")
            || msg.contains("rdf-star")
            || msg.contains("annotation")
            || msg.contains("not implemented")
            || msg.contains("unsupported"),
        "error must explain that RDF* evaluation is not implemented: {msg}"
    );
}

// ---------------------------------------------------------------------------
// URS-QEC-S03b — SPARQL XML results serialization
// ---------------------------------------------------------------------------

/// S03-b: The engine must support `application/sparql-results+xml` as a
/// result format.  The serialized output must be well-formed XML containing
/// the standard `<sparql>` root and `<results>` element with correct bindings.
#[tokio::test]
async fn xml_results_serialization_roundtrip() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update(
        "INSERT DATA { <http://ex/alice> <http://ex/name> \"Alice\" }",
        KS,
    )
    .await
    .expect("insert");

    let result = eng
        .execute("SELECT ?s ?name WHERE { ?s <http://ex/name> ?name }", KS)
        .await
        .expect("select");

    // Serialize to XML.
    let xml_bytes = result.to_xml().expect("XML serialization must succeed");
    let xml = String::from_utf8(xml_bytes).expect("XML must be valid UTF-8");

    // Must be well-formed SPARQL Results XML per W3C.
    assert!(
        xml.contains("<sparql") || xml.contains("sparql-results"),
        "XML must contain <sparql> root element: {xml}"
    );
    assert!(
        xml.contains("<results"),
        "XML must contain <results>: {xml}"
    );
    assert!(
        xml.contains("Alice"),
        "XML must include the bound literal: {xml}"
    );
}

/// S03-b: `ResultFormat::from_accept` must recognise
/// `application/sparql-results+xml` and route to XML serialization.
#[test]
fn result_format_accepts_xml_content_type() {
    use ferrosa_sparql::results::ResultFormat;
    let fmt = ResultFormat::from_accept("application/sparql-results+xml");
    assert_eq!(
        fmt,
        ResultFormat::Xml,
        "Accept: application/sparql-results+xml must select Xml format"
    );
    assert_eq!(
        fmt.content_type(),
        "application/sparql-results+xml",
        "Xml format must advertise the correct Content-Type"
    );
}

// ---------------------------------------------------------------------------
// URS-QEC-S04 — ORDER BY on expressions must fail loud
// ---------------------------------------------------------------------------

/// S04 (URS-QEC-X01): ORDER BY with a non-variable expression (e.g. a
/// function call like `STR(?name)`, or an arithmetic expression) must return a
/// clear SPARQL protocol error, NOT silently skip the ordering and return
/// results in arbitrary order.
///
/// Current (broken) behaviour: `planner.rs` logs a warning and falls through,
/// silently discarding the ORDER BY clause.  The caller gets results in an
/// arbitrary, undocumented order with no indication that the requested ordering
/// was not applied.
///
/// Required behaviour: the engine returns `Err(SparqlError::Plan(…))` so the
/// HTTP layer returns 400 Bad Request, not a silently unordered 200.
#[tokio::test]
async fn order_by_expression_fails_loud_not_silent_ignore() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    // The query itself must fail loud at planning time regardless of stored data.
    let err = eng
        .execute(
            "SELECT ?s ?name \
             WHERE  { ?s <http://ex/name> ?name } \
             ORDER BY STR(?name)",
            KS,
        )
        .await
        .expect_err(
            "ORDER BY on a function expression must return Err (fail loud), \
             not silently ignore the ordering clause and return Ok",
        );

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("order")
            || msg.contains("expression")
            || msg.contains("not implemented")
            || msg.contains("unsupported"),
        "error must explain that ORDER BY expressions are not implemented: {msg}"
    );
}

/// S04 (positive case): ORDER BY on a plain variable must still work correctly
/// after the fail-loud change — we must not regress variable-based ordering.
#[tokio::test]
async fn order_by_variable_still_works_after_fail_loud_change() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update(
        "INSERT DATA { \
            <http://ex/b> <http://ex/name> \"Berta\" . \
            <http://ex/a> <http://ex/name> \"Anna\"  . \
            <http://ex/c> <http://ex/name> \"Carl\" }",
        KS,
    )
    .await
    .expect("insert data");

    let names = select_values(
        &eng,
        "SELECT ?name WHERE { ?s <http://ex/name> ?name } ORDER BY ?name",
        "name",
    )
    .await;

    assert_eq!(
        names,
        vec!["Anna", "Berta", "Carl"],
        "ascending ORDER BY ?name must sort correctly"
    );
}

/// S04 (negative direction): ORDER BY DESC(expression) must also fail loud.
#[tokio::test]
async fn order_by_desc_expression_fails_loud() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    let err = eng
        .execute(
            "SELECT ?s ?name \
             WHERE  { ?s <http://ex/name> ?name } \
             ORDER BY DESC(STR(?name))",
            KS,
        )
        .await
        .expect_err("ORDER BY DESC(expr) must return Err (fail loud)");

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("order")
            || msg.contains("expression")
            || msg.contains("not implemented")
            || msg.contains("unsupported"),
        "error must mention unsupported ORDER BY expression: {msg}"
    );
}
