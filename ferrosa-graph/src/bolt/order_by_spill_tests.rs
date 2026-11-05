//! Correctness: an unbounded ORDER BY spills to disk and streams the merge,
//!   whether or not its terms are projected, and never leaks the hidden sort
//!   keys into the result.
//! Last revised: 2026-11-04
//! Last changed: Added with the hidden-sort-key ORDER BY (fix/stream-graph-order-by).
//!
//! End to end through `GraphEngine` — parse, plan, execute, transport stream —
//! with a spill backend whose sorter spills on every row, so "took the
//! spilling path" is observable as run files on disk.
use super::*;
use crate::executor::spill::test_support::{spilling_config, TempDirReserver};
use futures::StreamExt as _;

/// (name, age, team). Ages are distinct and deliberately not in insertion or
/// name order, so an unsorted or wrongly-sorted result cannot pass.
const PEOPLE: &[(&str, i64, &str)] = &[
    ("dana", 41, "red"),
    ("alice", 7, "blue"),
    ("erin", 23, "red"),
    ("bob", 65, "green"),
    ("cara", 12, "blue"),
    ("finn", 30, "green"),
];

struct SpillFixture {
    fixture: Fixture,
    reserver: Arc<TempDirReserver>,
    _spill_dir: tempfile::TempDir,
}

async fn spill_fixture() -> SpillFixture {
    let spill_dir = tempfile::tempdir().unwrap();
    let reserver = TempDirReserver::new(spill_dir.path());
    let fixture = Fixture::with_config(spilling_config(reserver.clone(), Default::default()));
    for (name, age, team) in PEOPLE {
        fixture
            .seed_engine()
            .execute(
                &format!("CREATE (n:Ranked {{name: '{name}', age: {age}, team: '{team}'}})"),
                "bolt_auto",
                &Fixture::auth(),
            )
            .await
            .unwrap();
    }
    SpillFixture {
        fixture,
        reserver,
        _spill_dir: spill_dir,
    }
}

/// The reference order: what the in-memory bindings sort produces for these
/// rows (distinct integer ages, so every comparator agrees).
fn names_by_age(descending: bool) -> Vec<&'static str> {
    let mut people = PEOPLE.to_vec();
    people.sort_by_key(|(_, age, _)| *age);
    if descending {
        people.reverse();
    }
    people.into_iter().map(|(name, _, _)| name).collect()
}

fn strings(rows: &[Vec<serde_json::Value>]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            assert_eq!(row.len(), 1, "a hidden sort key leaked into row {row:?}");
            row[0]
                .as_str()
                .unwrap_or_else(|| panic!("expected a string cell, got {row:?}"))
                .to_string()
        })
        .collect()
}

/// `RETURN n.name ORDER BY n.age`: the order term is NOT a projected column.
///
/// This used to make `SpillSortSink::try_new` return `Ok(None)` and the whole
/// result was sorted in memory by `sort_projected_rows_by_bindings` — a silent
/// widening of the memory bound. Now the age travels as a hidden key column, so
/// the sort spills, and the transport stream pulls the merge from disk.
#[tokio::test]
async fn order_by_a_non_projected_property_spills_and_streams_in_order() {
    let f = spill_fixture().await;
    let query = "MATCH (n:Ranked) RETURN n.name ORDER BY n.age";

    let (columns, mut rows, _stats) = f
        .fixture
        .engine
        .execute_stream_with_params(query, "bolt_auto", &Fixture::auth(), &HashMap::new())
        .await
        .unwrap();
    assert_eq!(columns, vec!["n.name".to_string()]);
    assert!(
        f.reserver.live_run_files() > 1,
        "ORDER BY on a non-projected property must spill sorted runs to disk \
         (reservations: {:?})",
        f.reserver.reserved_paths()
    );

    let mut streamed = Vec::new();
    while let Some(row) = rows.next().await {
        streamed.push(row.unwrap());
    }
    assert_eq!(strings(&streamed), names_by_age(false));
    drop(rows);
    assert_eq!(
        f.reserver.live_run_files(),
        0,
        "dropping the stream must remove the temp-sort directory"
    );

    let buffered = f
        .fixture
        .engine
        .execute(query, "bolt_auto", &Fixture::auth())
        .await
        .unwrap();
    assert_eq!(buffered.columns, vec!["n.name".to_string()]);
    assert_eq!(strings(&buffered.rows), names_by_age(false));
}

/// Hidden key columns must never reach the client: not in `columns`, not in
/// any row's width — for DISTINCT, LIMIT and both together, on both the
/// streaming and the buffered entry point.
#[tokio::test]
async fn hidden_sort_keys_never_appear_in_columns_or_rows() {
    let f = spill_fixture().await;
    /// (query, expected columns, expected rows).
    type Case = (&'static str, Vec<&'static str>, Vec<Vec<serde_json::Value>>);
    let cases: &[Case] = &[
        (
            "MATCH (n:Ranked) RETURN n.name ORDER BY n.age DESC LIMIT 2",
            vec!["n.name"],
            names_by_age(true)
                .into_iter()
                .take(2)
                .map(|n| vec![serde_json::json!(n)])
                .collect(),
        ),
        (
            // DISTINCT dedups the VISIBLE row. Were the age key still attached,
            // every row would be distinct and all six would come back.
            "MATCH (n:Ranked) RETURN DISTINCT n.team ORDER BY n.age",
            vec!["n.team"],
            vec![
                vec![serde_json::json!("blue")],
                vec![serde_json::json!("red")],
                vec![serde_json::json!("green")],
            ],
        ),
        (
            "MATCH (n:Ranked) RETURN DISTINCT n.team ORDER BY n.age DESC LIMIT 2",
            vec!["n.team"],
            vec![
                vec![serde_json::json!("green")],
                vec![serde_json::json!("red")],
            ],
        ),
        (
            // A projected term alongside a non-projected one.
            "MATCH (n:Ranked) RETURN n.team, n.name ORDER BY n.team, n.age DESC LIMIT 3",
            vec!["n.team", "n.name"],
            vec![
                vec![serde_json::json!("blue"), serde_json::json!("cara")],
                vec![serde_json::json!("blue"), serde_json::json!("alice")],
                vec![serde_json::json!("green"), serde_json::json!("bob")],
            ],
        ),
    ];

    for (query, expected_columns, expected_rows) in cases {
        let expected_columns: Vec<String> =
            expected_columns.iter().map(|c| c.to_string()).collect();

        let (columns, rows, _stats) = f
            .fixture
            .engine
            .execute_stream_with_params(query, "bolt_auto", &Fixture::auth(), &HashMap::new())
            .await
            .unwrap();
        let streamed = crate::executor::stream::collect_rows(rows).await.unwrap();
        assert_eq!(columns, expected_columns, "streaming columns for {query}");
        assert_eq!(&streamed, expected_rows, "streaming rows for {query}");

        let buffered = f
            .fixture
            .engine
            .execute(query, "bolt_auto", &Fixture::auth())
            .await
            .unwrap();
        assert_eq!(
            buffered.columns, expected_columns,
            "buffered columns for {query}"
        );
        assert_eq!(&buffered.rows, expected_rows, "buffered rows for {query}");
    }
    assert!(
        !f.reserver.reserved_paths().is_empty(),
        "every case has an ORDER BY, so the spill backend must have been used"
    );
}
