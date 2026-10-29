//! RED regressions for t_ec9e8531: relation incarnations must isolate PG MVCC.
//!
//! Test list: SQL recreation; unrelated retained snapshot; pending old writer;
//! recreation and pending writer through the shared CQL DDL execution boundary.
//! No CQL wire parser is involved: the latter tests invoke its shared DdlPath.

#[path = "common/pg_server.rs"]
mod pg_server;

use std::future::Future;
use std::time::Duration;

use ferrosa_cluster::pair::ddl::DdlOperation;
use ferrosa_schema::TableMetadata;
use pg_server::{connect, start_server_with_ddl, TestServer};
use tokio_postgres::{Client, SimpleQueryMessage};
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
enum DdlRoute {
    Sql,
    SharedCql,
}

async fn bounded(test: impl Future<Output = ()>) {
    tokio::time::timeout(Duration::from_secs(30), test)
        .await
        .expect("DROP/recreate regression exceeded 30 seconds");
}

async fn sql(client: &Client, statement: &str) {
    client
        .batch_execute(statement)
        .await
        .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
}

async fn rows(client: &Client, table: &str) -> Vec<(i32, i32)> {
    client
        .simple_query(&format!("SELECT id, value FROM {table} ORDER BY id, value"))
        .await
        .expect("SELECT rows")
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some((
                row.get(0).expect("id").parse().expect("integer id"),
                row.get(1).expect("value").parse().expect("integer value"),
            )),
            _ => None,
        })
        .collect()
}

fn metadata(server: &TestServer) -> TableMetadata {
    server.schema.snapshot().tables[&("ks".into(), "drop_probe".into())].clone()
}

async fn recreate(server: &TestServer, client: &Client, route: DdlRoute) {
    let old = metadata(server);
    match route {
        DdlRoute::Sql => {
            sql(client, "DROP TABLE drop_probe").await;
            sql(client, "CREATE TABLE drop_probe(id integer, value integer)").await;
        }
        DdlRoute::SharedCql => {
            server
                .ddl_path
                .load_full()
                .execute(DdlOperation::DropTable {
                    keyspace: "ks".into(),
                    table: "drop_probe".into(),
                })
                .await
                .expect("shared CQL DDL DROP");
            let mut replacement = old.clone();
            replacement.id = Uuid::new_v4();
            server
                .ddl_path
                .load_full()
                .execute(DdlOperation::CreateTable(Box::new(replacement)))
                .await
                .expect("shared CQL DDL CREATE");
        }
    }
    assert_ne!(
        old.id,
        metadata(server).id,
        "recreation needs a new schema ID"
    );
}

async fn recreation_excludes_old_rows(route: DdlRoute) {
    let server = start_server_with_ddl().await;
    let client = connect(server.port).await;
    // Explicit column lists are essential: the physical PK is synthetic.
    sql(
        &client,
        "CREATE TABLE drop_probe(id integer, value integer)",
    )
    .await;
    sql(&client, "INSERT INTO drop_probe(id, value) VALUES (1, 10)").await;
    assert_eq!(rows(&client, "drop_probe").await, vec![(1, 10)]);

    recreate(&server, &client, route).await;
    sql(&client, "INSERT INTO drop_probe(id, value) VALUES (1, 11)").await;

    // Check a fresh session too: the listener's MVCC manager is shared.
    let fresh = connect(server.port).await;
    let existing_rows = rows(&client, "drop_probe").await;
    let fresh_rows = rows(&fresh, "drop_probe").await;
    assert_eq!(
        (existing_rows, fresh_rows),
        (vec![(1, 11)], vec![(1, 11)]),
        "{route:?} recreation leaked the previous incarnation's MVCC images"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_recreation_excludes_old_synthetic_key_rows() {
    bounded(recreation_excludes_old_rows(DdlRoute::Sql)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_cql_ddl_recreation_excludes_old_synthetic_key_rows() {
    bounded(recreation_excludes_old_rows(DdlRoute::SharedCql)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_preserves_unrelated_rows_and_retained_snapshot() {
    bounded(async {
        let server = start_server_with_ddl().await;
        let client = connect(server.port).await;
        let reader = connect(server.port).await;
        sql(
            &client,
            "CREATE TABLE drop_probe(id integer, value integer)",
        )
        .await;
        sql(&client, "INSERT INTO drop_probe(id, value) VALUES (1, 10)").await;
        sql(
            &client,
            "CREATE TABLE keep_probe(id integer PRIMARY KEY, value integer)",
        )
        .await;
        sql(&client, "INSERT INTO keep_probe(id, value) VALUES (7, 70)").await;
        sql(&reader, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        assert_eq!(rows(&reader, "keep_probe").await, vec![(7, 70)]);
        sql(&client, "UPDATE keep_probe SET value = 71 WHERE id = 7").await;
        assert_eq!(rows(&reader, "keep_probe").await, vec![(7, 70)]);

        recreate(&server, &client, DdlRoute::Sql).await;

        // Clearing the entire manager would lose this unrelated before-image.
        assert_eq!(rows(&reader, "keep_probe").await, vec![(7, 70)]);
        assert_eq!(rows(&client, "keep_probe").await, vec![(7, 71)]);
        sql(&reader, "ROLLBACK").await;
        assert_eq!(rows(&reader, "keep_probe").await, vec![(7, 71)]);
    })
    .await;
}

async fn old_writer_cannot_commit_into_replacement(route: DdlRoute) {
    let server = start_server_with_ddl().await;
    let client = connect(server.port).await;
    let writer = connect(server.port).await;
    sql(
        &client,
        "CREATE TABLE drop_probe(id integer, value integer)",
    )
    .await;
    sql(
        &client,
        "CREATE TABLE keep_probe(id integer PRIMARY KEY, value integer)",
    )
    .await;
    sql(&client, "INSERT INTO keep_probe(id, value) VALUES (7, 70)").await;
    sql(&writer, "BEGIN").await;
    sql(&writer, "INSERT INTO drop_probe(id, value) VALUES (2, 20)").await;
    sql(&writer, "UPDATE keep_probe SET value = 99 WHERE id = 7").await;
    assert_eq!(rows(&writer, "drop_probe").await, vec![(2, 20)]);
    assert!(rows(&client, "drop_probe").await.is_empty());

    recreate(&server, &client, route).await;
    // Leave the new table empty: a new-row commit could accidentally hide the
    // missing incarnation check behind an ordinary table-epoch conflict.
    let commit = writer.batch_execute("COMMIT").await;
    let replacement_rows = rows(&client, "drop_probe").await;
    let unrelated_rows = rows(&client, "keep_probe").await;
    assert!(
        commit.is_err() && replacement_rows.is_empty() && unrelated_rows == vec![(7, 70)],
        "{route:?}: stale transaction must fail atomically; commit={commit:?}, \
         replacement={replacement_rows:?}, unrelated={unrelated_rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_drop_transaction_cannot_commit_into_sql_replacement() {
    bounded(old_writer_cannot_commit_into_replacement(DdlRoute::Sql)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_drop_transaction_cannot_commit_into_shared_cql_replacement() {
    bounded(old_writer_cannot_commit_into_replacement(
        DdlRoute::SharedCql,
    ))
    .await;
}

/// Pause inside the real storage apply, after admission but before MVCC publication.
/// Schema retirement must wait, even when invoked through a non-SQL route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schema_retirement_waits_for_inflight_postgres_commit() {
    bounded(async {
        use ferrosa_storage::observer::{ObserverMode, WriteObserver};
        use ferrosa_storage::{Mutation, TableId};
        use std::sync::{mpsc, Arc, Mutex};
        struct PauseApply {
            entered: mpsc::Sender<()>,
            release: Mutex<mpsc::Receiver<()>>,
        }
        impl WriteObserver for PauseApply {
            fn mode(&self) -> ObserverMode {
                ObserverMode::Sync
            }
            fn tables(&self) -> Vec<TableId> {
                vec![TableId::new("ks", "drop_probe")]
            }
            fn on_write(&self, _: &TableId, _: &Mutation) -> Vec<Mutation> {
                self.entered.send(()).unwrap();
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
                Vec::new()
            }
        }
        let server = start_server_with_ddl().await;
        let client = connect(server.port).await;
        sql(
            &client,
            "CREATE TABLE drop_probe(id integer, value integer)",
        )
        .await;
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        server.engine.register_observer(Arc::new(PauseApply {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        }));
        let writer = tokio::spawn(async move {
            client
                .batch_execute("INSERT INTO drop_probe(id, value) VALUES (1, 10)")
                .await
        });
        tokio::task::spawn_blocking(move || {
            entered_rx.recv_timeout(Duration::from_secs(10)).unwrap()
        })
        .await
        .unwrap();
        let schema = server.schema.clone();
        let (attempt_tx, attempt_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let dropper = std::thread::spawn(move || {
            attempt_tx.send(()).unwrap();
            schema.drop_table_internal("ks", "drop_probe").unwrap();
            done_tx.send(()).unwrap();
        });
        attempt_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let retired_during_apply = done_rx.recv_timeout(Duration::from_millis(200)).is_ok();
        // Always release before asserting, including on RED.
        release_tx.send(()).unwrap();
        writer.await.unwrap().unwrap();
        dropper.join().unwrap();
        assert!(
            !retired_during_apply,
            "schema retired a table while its PostgreSQL commit was still applying"
        );
    })
    .await;
}

#[test]
fn replica_rejects_legacy_metadata_without_table_incarnation() {
    use ferrosa_common::accord::{Timestamp, TxnId};
    use ferrosa_storage::accord::PostgresMvccApplyObserver;
    let mut message = capnp::message::Builder::new_default();
    let root =
        message.init_root::<ferrosa_net::protocol::envelope_capnp::pg_mvcc_row_changes::Builder>();
    let mut change = root.init_changes(1).get(0);
    change.set_table("ks.drop_probe");
    // Deliberately encode the old wire form: no table UUID field.
    let payload = capnp::serialize::write_message_to_words(&message);
    let manager = ferrosa_postgres::MvccManager::default();
    let t = Timestamp::synthetic(100);
    assert!(
        manager
            .prepare_postgres_apply(TxnId::new(1, t), t, &[payload])
            .is_err(),
        "legacy row versions must not attach to an arbitrary current incarnation"
    );
}

fn replica_write(
    table: &TableMetadata,
    id: i32,
    value: i32,
    t: ferrosa_common::accord::Timestamp,
) -> ferrosa_cluster::accord::apply::ApplyMutation {
    use ferrosa_common::CqlValue;
    let key = ferrosa_row_bridge::build_decorated_key(&[CqlValue::Int(id)], &[]).unwrap();
    let row = ferrosa_row_bridge::build_row(&[(0, CqlValue::Int(value))], &[], 1, None);
    let mutation = ferrosa_storage::Mutation::new(
        table.keyspace.clone(),
        table.name.clone(),
        key,
        vec![row],
        1,
    );
    let mut bytes = vec![0; mutation.serialized_size()];
    mutation.serialize_into(&mut bytes);
    let mut message = capnp::message::Builder::new_default();
    let mut root =
        message.init_root::<ferrosa_net::protocol::envelope_capnp::pg_mvcc_row_changes::Builder>();
    root.set_table(format!("{}.{}", table.keyspace, table.name));
    root.set_table_id(table.id.as_bytes());
    root.init_changes(0); // relation-only metadata still fences admission without row images
    let metadata = capnp::serialize::write_message_to_words(&message);
    ferrosa_cluster::accord::apply::ApplyMutation {
        data: ferrosa_storage::accord::encode_postgres_mvcc_mutation(&bytes, &metadata).unwrap(),
        t,
        deps: vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_replica_and_replay_reject_stale_batch_atomically() {
    bounded(async {
        use ferrosa_cluster::accord::apply::{EngineStorageApplier, StorageApplier};
        use ferrosa_common::{
            accord::{Timestamp, TxnId},
            CqlValue,
        };
        use std::sync::Arc;
        let server = start_server_with_ddl().await;
        let client = connect(server.port).await;
        sql(
            &client,
            "CREATE TABLE drop_probe(id integer PRIMARY KEY, value integer)",
        )
        .await;
        sql(
            &client,
            "CREATE TABLE keep_probe(id integer PRIMARY KEY, value integer)",
        )
        .await;
        let old = metadata(&server);
        let keep = server.schema.snapshot().tables[&("ks".into(), "keep_probe".into())].clone();
        let manager = Arc::new(ferrosa_postgres::MvccManager::default());
        manager.attach_schema(server.schema.clone());
        let applier = EngineStorageApplier::new(server.engine.clone());
        applier
            .register_postgres_mvcc_observer(manager.clone())
            .unwrap();
        let t = Timestamp::synthetic(4_000_000_000_000_000_000);
        let txn = TxnId::new(1, t);
        let current = vec![
            replica_write(&old, 1, 10, t),
            replica_write(&keep, 7, 70, t),
        ];
        applier.apply_writeset(txn, current.clone()).unwrap();
        applier.apply_writeset(txn, current.clone()).unwrap();
        assert_eq!(
            applier.applied_count(),
            2,
            "replay within the same incarnation is idempotent"
        );
        recreate(&server, &client, DdlRoute::SharedCql).await;
        let keep_key = ferrosa_row_bridge::build_decorated_key(&[CqlValue::Int(7)], &[]).unwrap();
        let keep_tid = ferrosa_storage::TableId::new("ks", "keep_probe");
        let before = format!("{:?}", server.engine.read(&keep_tid, &keep_key).unwrap());
        let later = Timestamp::synthetic(t.time + 1_000_000);
        let stale = vec![
            replica_write(&old, 2, 20, later),
            replica_write(&keep, 7, 99, later),
        ];
        assert!(applier.apply_writeset(TxnId::new(1, later), stale).is_err());
        // A fresh replica applier has no in-memory dedupe entries: old durable
        // Accord metadata still must not replay into the replacement.
        let replay = EngineStorageApplier::new(server.engine.clone());
        replay.register_postgres_mvcc_observer(manager).unwrap();
        assert!(replay.apply_writeset(txn, current).is_err());
        assert!(rows(&client, "drop_probe").await.is_empty());
        assert_eq!(
            format!("{:?}", server.engine.read(&keep_tid, &keep_key).unwrap()),
            before
        );
        assert_eq!(replay.applied_count(), 0);
    })
    .await;
}
