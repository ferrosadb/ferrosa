//! Module: spilled Expand anchor states.
//! Correctness: the anchor scan of a MATCH never holds its surviving states in
//!   a `Vec`: past `spill_threshold_bytes` they spill to a run in scan order,
//!   and hops, WITH, OPTIONAL MATCH and the projection consume that run on
//!   pull with exactly the results the in-memory path produced.
//! Last revised: 2026-11-16
//! Last changed: Added with the spilled anchor frontier (fix/oom-graph-stream).
use super::*;
use crate::executor::spill::test_support::{spilling_config, TempDirReserver};

const PEOPLE: i64 = 12;

async fn seed(fixture: &Fixture) {
    let engine = fixture.seed_engine();
    for i in 0..PEOPLE {
        engine
            .execute(
                &format!("CREATE (n:Walker {{name: 'w{i:02}', step: {i}}})"),
                "bolt_auto",
                &Fixture::auth(),
            )
            .await
            .unwrap();
    }
    for i in 0..PEOPLE - 1 {
        let next = i + 1;
        engine
            .execute(
                &format!(
                    "MERGE (a:Walker {{name: 'w{i:02}'}})-[r:NEXT]->(b:Walker {{name: 'w{next:02}'}}) \
                     RETURN r"
                ),
                "bolt_auto",
                &Fixture::auth(),
            )
            .await
            .unwrap();
    }
}

const QUERIES: &[&str] = &[
    "MATCH (n:Walker) RETURN n.name",
    "MATCH (n:Walker) RETURN n.name LIMIT 4",
    "MATCH (n:Walker) RETURN DISTINCT n.name ORDER BY n.name DESC",
    "MATCH (a:Walker)-[:NEXT]->(b:Walker) RETURN a.name, b.name",
    "MATCH (a:Walker)-[:NEXT]->(b:Walker)-[:NEXT]->(c:Walker) RETURN a.name, c.name",
    "MATCH (n:Walker) WITH n.step AS s WHERE s >= 5 RETURN s ORDER BY s",
    "MATCH (n:Walker) WITH n.step AS s ORDER BY s DESC LIMIT 3 RETURN s",
    "MATCH (n:Walker) WITH DISTINCT n.step AS m WHERE m < 4 RETURN m ORDER BY m",
    "MATCH (n:Walker) OPTIONAL MATCH (n)-[:NEXT]->(m:Walker) RETURN n.name, m.name",
];

fn sorted(mut rows: Vec<Vec<serde_json::Value>>) -> Vec<String> {
    let mut out: Vec<String> = rows.drain(..).map(|row| format!("{row:?}")).collect();
    out.sort();
    out
}

/// Every query shape that consumes the anchor frontier returns the same rows
/// whether the anchor states stay in memory or spill on every state; the
/// spilling engine provably reserved an anchor-state run.
#[tokio::test]
async fn spilled_anchor_states_match_the_in_memory_frontier() {
    let reference = Fixture::new();
    seed(&reference).await;

    let spill_dir = tempfile::tempdir().unwrap();
    let reserver = TempDirReserver::new(spill_dir.path());
    let spilling = Fixture::with_config(spilling_config(reserver.clone(), Default::default()));
    seed(&spilling).await;

    for query in QUERIES {
        let expected = reference
            .engine
            .execute(query, "bolt_auto", &Fixture::auth())
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let got = spilling
            .engine
            .execute(query, "bolt_auto", &Fixture::auth())
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        assert_eq!(got.columns, expected.columns, "{query}");
        if query.contains("ORDER BY") {
            assert_eq!(got.rows, expected.rows, "{query}");
        } else if query.contains("LIMIT") {
            assert_eq!(got.rows.len(), expected.rows.len(), "{query}");
        } else {
            assert_eq!(sorted(got.rows), sorted(expected.rows), "{query}");
        }
        assert_eq!(
            got.stats.vertices_read, expected.stats.vertices_read,
            "{query}: stats must be complete at setup"
        );
    }

    let anchor_runs = reserver
        .reserved_paths()
        .iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("graph_anchor_states"))
        })
        .count();
    assert!(
        anchor_runs >= QUERIES.len(),
        "every MATCH must route its anchor states through a spill run, got {anchor_runs}"
    );
}
