//! URS-QEC-S02 — Full SPARQL UPDATE graph-management forms beyond M1 deletes
//! and the already-landed INSERT … WHERE: CREATE, LOAD, and the desugared
//! ADD / MOVE / COPY operations.
//!
//! RED tests (written before implementation). Each documents its requirement
//! and its fail-loud expectation (URS-QEC-X01): any form this engine does not
//! genuinely implement must return a SPARQL error, never a silent wrong/empty
//! result.
//!
//! ## Scope decisions (single-graph-per-keyspace model)
//!
//! The `rdf_triples` table is keyed `((graph, subject), predicate, object)` but
//! the engine's read path (FullScan / range_read) does not filter by the graph
//! partition-key component, and the table id is derived from the keyspace. So
//! *named graphs distinct from the keyspace's default graph are not addressable*
//! for reads or writes. Operations that target/read such a named graph MUST
//! fail loud rather than silently read/write the wrong graph.
//!
//! | Form | Behavior |
//! |---|---|
//! | `CREATE GRAPH <g>` | success no-op (graphs are implicit; the graph exists after) |
//! | `LOAD <src> [INTO GRAPH <g>]` | fail loud — no RDF document fetch/parse pipeline |
//! | `ADD/MOVE/COPY` touching a named graph ≠ keyspace | fail loud — named graph not addressable |

use std::sync::Arc;

use ferrosa_cluster::write_path::WritePath;
use ferrosa_sparql::engine::{SparqlConfig, SparqlEngine};
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

// ---------------------------------------------------------------------------
// CREATE — implemented as a success no-op (graphs are implicit).
// ---------------------------------------------------------------------------

/// S02-create: `CREATE GRAPH <g>` must succeed (graphs are implicit in the
/// single-graph-per-keyspace model — the graph "exists" after the call).
#[tokio::test]
async fn create_graph_succeeds_as_noop() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    let res = eng
        .execute_update("CREATE GRAPH <http://ex/g1>", KS)
        .await
        .expect("CREATE GRAPH must succeed");
    assert_eq!(res.triples_inserted, 0);
    assert_eq!(res.triples_deleted, 0);
}

/// S02-create-silent: `CREATE SILENT GRAPH <g>` must also succeed.
#[tokio::test]
async fn create_silent_graph_succeeds() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update("CREATE SILENT GRAPH <http://ex/g1>", KS)
        .await
        .expect("CREATE SILENT GRAPH must succeed");
}

/// S02-create-then-insert: after CREATE, INSERT DATA into the default graph
/// still works (CREATE did not corrupt or block subsequent writes).
#[tokio::test]
async fn create_then_insert_data_works() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update("CREATE GRAPH <http://ex/g1>", KS)
        .await
        .expect("create");
    let res = eng
        .execute_update(
            "INSERT DATA { <http://ex/a> <http://ex/p> <http://ex/b> }",
            KS,
        )
        .await
        .expect("insert after create");
    assert_eq!(res.triples_inserted, 1);
}

// ---------------------------------------------------------------------------
// LOAD — fail loud (no RDF document fetch/parse pipeline).
// ---------------------------------------------------------------------------

/// S02-load: `LOAD <src>` must fail loud — the engine has no HTTP fetch + RDF
/// parser, so it cannot honor LOAD. It must NOT silently succeed with zero
/// triples (URS-QEC-X01).
#[tokio::test]
async fn load_fails_loud() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    let err = eng
        .execute_update("LOAD <http://example.org/data.ttl>", KS)
        .await
        .expect_err("LOAD must fail loud, not silently succeed");
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("load"),
        "LOAD error must mention LOAD: got {err}"
    );
}

/// S02-load-into: `LOAD <src> INTO GRAPH <g>` must also fail loud.
#[tokio::test]
async fn load_into_graph_fails_loud() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update(
        "LOAD <http://example.org/data.ttl> INTO GRAPH <http://ex/g>",
        KS,
    )
    .await
    .expect_err("LOAD INTO GRAPH must fail loud");
}

// ---------------------------------------------------------------------------
// ADD / MOVE / COPY — must NOT silently write to / read from the wrong graph.
// spargebra desugars these into DeleteInsert (+ Drop). When the desugaring
// touches a named graph ≠ keyspace, the engine must fail loud rather than
// silently operate on the default graph.
// ---------------------------------------------------------------------------

/// S02-copy-named-target: `COPY DEFAULT TO <g>` desugars to an INSERT whose
/// target QuadPattern graph is the named graph `<g>`. Since `<g>` is not
/// addressable, this must fail loud — NOT silently write the triples back into
/// the default graph (which would be a silent wrong result).
#[tokio::test]
async fn copy_to_named_graph_fails_loud_not_silent_default_write() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update(
        "INSERT DATA { <http://ex/a> <http://ex/p> <http://ex/b> }",
        KS,
    )
    .await
    .expect("seed");

    let err = eng
        .execute_update("COPY DEFAULT TO <http://ex/g2>", KS)
        .await
        .expect_err("COPY to a named graph must fail loud, not silently no-op into default");
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("graph"),
        "error must explain the named-graph limitation: got {err}"
    );
}

/// S02-copy-named-source: `COPY <g1> TO DEFAULT` desugars to a DeleteInsert
/// whose WHERE pattern reads from named graph `<g1>` (GraphPattern::Graph).
/// Reading a non-default named graph is unsupported and must fail loud.
#[tokio::test]
async fn copy_from_named_graph_fails_loud() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update("COPY <http://ex/g1> TO DEFAULT", KS)
        .await
        .expect_err("COPY from a named graph must fail loud (named graph not readable)");
}

/// S02-add-named: `ADD DEFAULT TO <g>` must fail loud for the same reason as
/// COPY (named insert target).
#[tokio::test]
async fn add_to_named_graph_fails_loud() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update("ADD DEFAULT TO <http://ex/g>", KS)
        .await
        .expect_err("ADD to a named graph must fail loud");
}

/// S02-move-named: `MOVE DEFAULT TO <g>` must fail loud (it desugars to
/// Drop(<g>) + copy into <g> + Drop(default); the copy into the named graph is
/// not addressable).
#[tokio::test]
async fn move_to_named_graph_fails_loud() {
    let (storage, wp, _dir) = setup();
    let eng = engine(storage, wp);

    eng.execute_update("MOVE DEFAULT TO <http://ex/g>", KS)
        .await
        .expect_err("MOVE to a named graph must fail loud");
}
