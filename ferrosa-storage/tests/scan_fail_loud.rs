//! The scan contract keeps *table-absent* and *table-empty* distinguishable.
//!
//! A table that is not registered on this node scans as `Err(NoSuchTable)` —
//! the stream's single item, then end of stream. It must never be an empty
//! stream, which a caller cannot tell apart from a table that legitimately has
//! no rows: for a JOIN that silently drops one side and returns a
//! plausible-but-wrong result.
//!
//! A registered table with no rows stays `Ok`: the scan yields nothing and
//! raises nothing.
//!
//! Tracks FM-41 / R15 —
//! `specs/todo/postgres-frontend/todo/storage-scan-fail-loud.md`.

use ferrosa_common::schema::{ColumnDefinition, TableSchema};
use ferrosa_common::Error;
use ferrosa_storage::{StorageEngine, StorageEngineConfig, TableId};
use futures::StreamExt as _;

const KEYSPACE: &str = "scan_fail_loud";

fn schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.into(),
        table: "t".into(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
        clustering_columns: vec![ColumnDefinition {
            name: "ck".into(),
            type_name: "org.apache.cassandra.db.marshal.Int32Type".into(),
        }],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "v".into(),
            type_name: "org.apache.cassandra.db.marshal.UTF8Type".into(),
        }],
        extensions: Default::default(),
    }
}

fn open_engine(dir: &std::path::Path) -> StorageEngine {
    StorageEngine::new(StorageEngineConfig::test_config(dir), None).unwrap()
}

/// Assert the scan refused a missing table: exactly one item, a typed
/// `NoSuchTable` naming the table, and then the stream ends.
///
/// Generic over the row type so this covers every streaming scan entry point
/// without naming `Partition`.
async fn assert_missing_table<S, T>(mut stream: S, expected: &str)
where
    S: futures::stream::Stream<Item = ferrosa_common::Result<T>> + Unpin,
{
    match stream.next().await {
        None => panic!("scan of an unregistered table returned an empty stream"),
        Some(Ok(_)) => panic!("scan of an unregistered table returned Ok"),
        Some(Err(Error::NoSuchTable { table })) => {
            assert_eq!(table, expected, "the error must name the missing table");
        }
        Some(Err(other)) => {
            panic!("expected NoSuchTable for a table that is not registered, got {other:?}")
        }
    }
    assert!(
        stream.next().await.is_none(),
        "the missing-table error must terminate the scan, not be followed by rows"
    );
}

/// Assert the scan of a registered, row-less table succeeded with no rows.
async fn assert_scans_empty<S, T>(mut stream: S)
where
    S: futures::stream::Stream<Item = ferrosa_common::Result<T>> + Unpin,
{
    let mut rows = 0usize;
    while let Some(item) = stream.next().await {
        match item {
            Ok(_) => rows += 1,
            Err(error) => panic!("a registered table with no rows must not error: {error}"),
        }
    }
    assert_eq!(rows, 0, "a registered table with no rows yields nothing");
}

/// Every streaming scan entry point answers a missing table with the typed
/// error. The Postgres read path scans through `range_iter`, the coordinator
/// through the projected/fragmented variants, so all four carry the contract.
#[tokio::test]
async fn an_unregistered_table_is_a_typed_error_on_every_scan_path() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path());
    let absent = TableId::new(KEYSPACE, "absent");
    let expected = "scan_fail_loud.absent";

    assert_missing_table(engine.range_iter(&absent, None, None), expected).await;
    assert_missing_table(engine.range_iter_fragmented(&absent, None, None), expected).await;
    assert_missing_table(
        engine.range_iter_projected(&absent, vec![0], None, None, None),
        expected,
    )
    .await;
    assert_missing_table(
        engine.range_iter_projected_fragmented(&absent, vec![0], None, None),
        expected,
    )
    .await;
}

/// The other half of the contract: a table that IS registered and has no rows
/// is `Ok` with zero rows, and must stay distinguishable from the case above.
#[tokio::test]
async fn a_registered_table_with_no_rows_scans_as_ok_and_empty() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path());
    engine.register_table(schema()).unwrap();
    let empty = TableId::new(KEYSPACE, "t");

    assert_scans_empty(engine.range_iter(&empty, None, None)).await;
    assert_scans_empty(engine.range_iter_fragmented(&empty, None, None)).await;
    assert_scans_empty(engine.range_iter_projected(&empty, vec![0], None, None, None)).await;
    assert_scans_empty(engine.range_iter_projected_fragmented(&empty, vec![0], None, None)).await;
}

/// Registration is what flips the outcome: the same table scans empty while
/// registered and errors once it is dropped, so the distinction tracks storage
/// state rather than the query.
#[tokio::test]
async fn dropping_a_table_turns_its_scan_into_the_typed_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path());
    engine.register_table(schema()).unwrap();
    let table = TableId::new(KEYSPACE, "t");

    assert_scans_empty(engine.range_iter(&table, None, None)).await;

    engine.unregister_table(&table).unwrap();

    assert_missing_table(engine.range_iter(&table, None, None), "scan_fail_loud.t").await;
}
