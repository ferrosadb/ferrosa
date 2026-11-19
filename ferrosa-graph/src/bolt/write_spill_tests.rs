//! Module: read-then-write spill for graph write operators.
//! Correctness: SET / REMOVE / DELETE read their whole match set BEFORE the
//!   first write (no Halloween re-match), and that match set spills to disk
//!   past `spill_threshold_bytes` rather than being held in a `Vec`.
//! Last revised: 2026-11-16
//! Last changed: Added with the spilled write match set (fix/oom-graph-stream).
//!
//! End to end through `GraphEngine` with a spill backend whose sorter spills
//! on every row, so "the match set spilled" is observable as a reservation
//! labelled for the operator, and "it was spilled before the first write" is
//! observable from the SET write probe.
use super::*;
use crate::executor::spill::test_support::{spilling_config, TempDirReserver};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

const VERTICES: i64 = 40;

struct WriteFixture {
    fixture: Fixture,
    reserver: Arc<TempDirReserver>,
    _spill_dir: tempfile::TempDir,
}

async fn write_fixture() -> WriteFixture {
    let spill_dir = tempfile::tempdir().unwrap();
    let reserver = TempDirReserver::new(spill_dir.path());
    let fixture = Fixture::with_config(spilling_config(reserver.clone(), Default::default()));
    for i in 0..VERTICES {
        fixture
            .seed_engine()
            .execute(
                &format!("CREATE (n:Counter {{name: 'c{i}', hits: {i}, tag: 't'}})"),
                "bolt_auto",
                &Fixture::auth(),
            )
            .await
            .unwrap();
    }
    WriteFixture {
        fixture,
        reserver,
        _spill_dir: spill_dir,
    }
}

fn reservations_labelled(reserver: &TempDirReserver, label: &str) -> usize {
    reserver
        .reserved_paths()
        .iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(label))
        })
        .count()
}

async fn hits_by_name(f: &WriteFixture) -> Vec<(String, i64)> {
    let result = f
        .fixture
        .engine
        .execute(
            "MATCH (n:Counter) RETURN n.name, n.hits",
            "bolt_auto",
            &Fixture::auth(),
        )
        .await
        .unwrap();
    let mut rows: Vec<(String, i64)> = result
        .rows
        .iter()
        .map(|row| {
            (
                row[0].as_str().unwrap().to_string(),
                row[1].as_i64().unwrap(),
            )
        })
        .collect();
    rows.sort();
    rows
}

/// Records, at the first property write, how many match-set reservations had
/// already been made: a SET that writes while still scanning reaches its
/// first write before any match run exists.
#[derive(Default)]
struct FirstWriteProbe {
    reserver: std::sync::OnceLock<Arc<TempDirReserver>>,
    match_runs_at_first_write: std::sync::Mutex<Option<usize>>,
    writes: AtomicUsize,
}

#[async_trait::async_trait]
impl crate::engine::QueryExecutionTestProbe for FirstWriteProbe {
    fn after_poll(&self, _ready: bool) {}

    async fn before_property_write(&self) {
        if self.writes.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
            let reserver = self.reserver.get().expect("probe has a reserver");
            *self.match_runs_at_first_write.lock().unwrap() =
                Some(reservations_labelled(reserver, "graph_set_match"));
        }
    }
}

/// SET over a match set larger than the spill threshold: the match set spills
/// to a run on disk, the run is complete before the first write, and every
/// matched vertex is updated exactly once (`n.hits + 1000` applied twice to
/// one vertex, or a vertex re-matched after its own write, would show).
#[tokio::test]
async fn set_spills_its_match_set_before_the_first_write() {
    let f = write_fixture().await;
    let before = hits_by_name(&f).await;
    let query = "MATCH (n:Counter) WHERE n.hits < 1000 SET n.hits = n.hits + 1000";

    let probe = Arc::new(FirstWriteProbe::default());
    probe
        .reserver
        .set(f.reserver.clone())
        .unwrap_or_else(|_| panic!("reserver set once"));
    f.fixture
        .engine
        .set_query_execution_probe_for_test(query, probe.clone());
    let result = f
        .fixture
        .engine
        .execute(query, "bolt_auto", &Fixture::auth())
        .await
        .unwrap();

    assert_eq!(result.stats.vertices_written, VERTICES as usize);
    assert_eq!(probe.writes.load(AtomicOrdering::SeqCst), VERTICES as usize);
    assert_eq!(
        *probe.match_runs_at_first_write.lock().unwrap(),
        Some(1),
        "the SET match set must be spilled to its own run before the first write"
    );
    let after = hits_by_name(&f).await;
    let expected: Vec<(String, i64)> = before
        .into_iter()
        .map(|(name, hits)| (name, hits + 1000))
        .collect();
    assert_eq!(after, expected, "every vertex updated exactly once");
}

/// SET ... RETURN: the post-SET rows come back through a spill-backed result
/// (ORDER BY / LIMIT applied on the merged stream), identical to what the
/// in-memory path returned.
#[tokio::test]
async fn set_with_return_streams_its_projected_rows_from_the_spill() {
    let f = write_fixture().await;
    let result = f
        .fixture
        .engine
        .execute(
            "MATCH (n:Counter) SET n.hits = n.hits + 1 RETURN n.name, n.hits \
             ORDER BY n.hits DESC LIMIT 3",
            "bolt_auto",
            &Fixture::auth(),
        )
        .await
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![serde_json::json!("c39"), serde_json::json!(40)],
            vec![serde_json::json!("c38"), serde_json::json!(39)],
            vec![serde_json::json!("c37"), serde_json::json!(38)],
        ]
    );
    assert_eq!(reservations_labelled(&f.reserver, "graph_set_match"), 1);
    assert_eq!(reservations_labelled(&f.reserver, "graph_set_return"), 1);
}

/// REMOVE spills its match set the same way and clears every matched vertex.
#[tokio::test]
async fn remove_spills_its_match_set_and_clears_every_vertex() {
    let f = write_fixture().await;
    let result = f
        .fixture
        .engine
        .execute(
            "MATCH (n:Counter) REMOVE n.tag",
            "bolt_auto",
            &Fixture::auth(),
        )
        .await
        .unwrap();
    assert_eq!(result.stats.vertices_written, VERTICES as usize);
    assert_eq!(reservations_labelled(&f.reserver, "graph_remove_match"), 1);
    let tagged = f
        .fixture
        .engine
        .execute(
            "MATCH (n:Counter) WHERE n.tag = 't' RETURN n.name",
            "bolt_auto",
            &Fixture::auth(),
        )
        .await
        .unwrap();
    assert!(tagged.rows.is_empty(), "left tagged: {:?}", tagged.rows);
}

/// DELETE spills its match set, then tombstones every matched vertex.
#[tokio::test]
async fn delete_spills_its_match_set_and_deletes_every_vertex() {
    let f = write_fixture().await;
    let result = f
        .fixture
        .engine
        .execute("MATCH (n:Counter) DELETE n", "bolt_auto", &Fixture::auth())
        .await
        .unwrap();
    assert_eq!(result.stats.vertices_deleted, VERTICES as usize);
    assert_eq!(reservations_labelled(&f.reserver, "graph_delete_match"), 1);
    assert!(hits_by_name(&f).await.is_empty());
}
