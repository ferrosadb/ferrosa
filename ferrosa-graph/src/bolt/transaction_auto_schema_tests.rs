//! Correctness: transaction failures publish neither schema columns nor staged rows.
//! Last revised: 2026-10-26
//! Last changed: Guard first-write scalar DDL inside explicit Bolt transactions.
//! Explicit transactions must reject unstaged column DDL before publication.
use super::*;

#[tokio::test]
async fn explicit_transaction_rejects_new_scalar_column_before_ddl_and_reset_releases_writer() {
    let fixture = Fixture::new();
    let mut writer = fixture.connection();
    writer.authenticated = true;
    fixture
        .query(&mut writer, "CREATE (n:TxSchema {pk: 1, value: 'kept'})")
        .await;
    let replies = process_message(
        BoltMessage::Begin { extra: Vec::new() },
        &fixture.engine,
        &fixture.schema,
        &mut writer,
    )
    .await
    .unwrap();
    assert!(
        matches!(replies.as_slice(), [BoltMessage::Success { .. }]),
        "BEGIN after CREATE (n:TxSchema {{pk: 1, value: 'kept'}}) failed: {replies:?}"
    );
    fixture
        .query(
            &mut writer,
            "CREATE (n:TxSchema {pk: 99, value: 'pending'})",
        )
        .await;
    let replies = process_message(
        BoltMessage::Run {
            query: "MATCH (n:TxSchema {pk: 1}) SET n.new_scalar = 42".into(),
            params: Vec::new(),
            extra: Vec::new(),
        },
        &fixture.engine,
        &fixture.schema,
        &mut writer,
    )
    .await
    .unwrap();

    // Always release the transaction before checking the rejection, so the
    // deliberately RED version cannot leave admission owned by this connection.
    let reset = process_message(
        BoltMessage::Reset,
        &fixture.engine,
        &fixture.schema,
        &mut writer,
    )
    .await
    .unwrap();
    assert!(
        matches!(reset.as_slice(), [BoltMessage::Success { .. }]),
        "RESET after MATCH (n:TxSchema {{pk: 1}}) SET n.new_scalar = 42 failed: {reset:?}"
    );
    let rows = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        fixture
            .query(
                &mut writer,
                "CREATE (n:TxSchema {pk: 2, value: 'after reset'})",
            )
            .await;
        fixture
            .query(
                &mut writer,
                "MATCH (n:TxSchema) RETURN n.pk, n.value ORDER BY n.pk",
            )
            .await
    })
    .await
    .expect("RESET must release writer ownership");
    assert_eq!(
        rows,
        vec![
            vec![PackValue::Int(1), PackValue::String("kept".into())],
            vec![PackValue::Int(2), PackValue::String("after reset".into())],
        ],
        "RESET must discard pending rows and preserve committed data"
    );
    assert!(
        matches!(replies.as_slice(), [BoltMessage::Failure { .. }]),
        "explicit SET introducing a scalar column must reject before schema DDL: {replies:?}"
    );
    assert_eq!(
        fixture.coordinator.columns.load(Ordering::Relaxed),
        0,
        "transactional SET must not issue column DDL"
    );
    let snapshot = fixture.schema.snapshot();
    let table = snapshot
        .tables
        .values()
        .find(|table| {
            table.keyspace == "bolt_auto"
                && table
                    .extensions
                    .get("graph.label")
                    .is_some_and(|label| label == "TxSchema")
        })
        .expect("the pre-existing graph label must remain");
    assert!(
        !table.columns.contains_key("new_scalar"),
        "RESET/rollback cannot undo published column metadata"
    );
}
