//! Native PostgreSQL driver history test for distributed MVCC transactions.
//!
//! Run with `--features postgres-jepsen` and a semicolon-separated list of at
//! least three native PostgreSQL connection URLs in
//! `FERROSA_TEST_POSTGRES_URLS`. The feature is opt-in because it requires a
//! live Ferrosa cluster with the PostgreSQL protocol enabled.

#![cfg(feature = "postgres-jepsen")]

use anyhow::{bail, Context, Result};
use ferrosa_jepsen::checker::strict_serializable::{
    check_strict_serializable, RecordedTransaction, TransactionOperation,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const ACTORS: usize = 5;
const TRANSACTIONS_PER_ACTOR: usize = 4;
const INITIAL_BALANCE: i64 = 10_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[cfg(feature = "postgres-jepsen")]
async fn postgres_transactions_are_strictly_serializable() -> Result<()> {
    let urls = std::env::var("FERROSA_TEST_POSTGRES_URLS").context(
        "set FERROSA_TEST_POSTGRES_URLS to at least three semicolon-separated PostgreSQL URLs",
    )?;
    let urls: Vec<_> = urls
        .split(';')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .collect();
    if urls.len() < 3 {
        bail!(
            "FERROSA_TEST_POSTGRES_URLS must contain at least three node URLs, got {}",
            urls.len()
        );
    }

    let (clients, connections) = connect_all(&urls).await?;
    let table = format!("pg_ssi_{}", Uuid::new_v4().simple());
    clients[0]
        .batch_execute(&format!(
            "CREATE TABLE {table} (id TEXT PRIMARY KEY, balance BIGINT NOT NULL);\
             INSERT INTO {table} (id, balance) VALUES ('a', {INITIAL_BALANCE}), ('b', {INITIAL_BALANCE});"
        ))
        .await
        .context("create and initialize strict-serializability workload table")?;
    wait_for_table_on_all_nodes(&clients, &table).await?;

    let event_clock = Arc::new(AtomicU64::new(1));
    let history = Arc::new(Mutex::new(Vec::with_capacity(
        ACTORS * TRANSACTIONS_PER_ACTOR,
    )));
    let mut actors = Vec::with_capacity(ACTORS);
    for actor in 0..ACTORS {
        let client = Arc::clone(&clients[actor % clients.len()]);
        let event_clock = Arc::clone(&event_clock);
        let history = Arc::clone(&history);
        let table = table.clone();
        actors.push(tokio::spawn(async move {
            for iteration in 0..TRANSACTIONS_PER_ACTOR {
                let id = (actor * TRANSACTIONS_PER_ACTOR + iteration) as u64;
                let transaction = transfer_once(&client, &table, id, &event_clock).await?;
                history
                    .lock()
                    .expect("history mutex poisoned")
                    .push(transaction);
            }
            Result::<()>::Ok(())
        }));
    }
    for actor in actors {
        actor.await.context("join PostgreSQL workload actor")??;
    }

    let (final_a, final_b) = read_balances(&clients[0], &table).await?;
    let final_state = BTreeMap::from([("a".to_owned(), final_a), ("b".to_owned(), final_b)]);
    let initial = BTreeMap::from([
        ("a".to_owned(), INITIAL_BALANCE),
        ("b".to_owned(), INITIAL_BALANCE),
    ]);
    let mut history = history.lock().expect("history mutex poisoned").clone();
    history.sort_by_key(|transaction| transaction.id);
    assert_eq!(
        history.len(),
        ACTORS * TRANSACTIONS_PER_ACTOR,
        "every invoked operation must have a recorded completion"
    );
    assert!(
        history
            .iter()
            .filter(|transaction| transaction.committed)
            .count()
            >= 2,
        "the workload must commit multiple transactions to exercise concurrency"
    );

    for (node, client) in clients.iter().enumerate().skip(1) {
        let node_state = read_balances(client, &table)
            .await
            .with_context(|| format!("read final balances from PostgreSQL node {node}"))?;
        assert_eq!(
            node_state,
            (final_a, final_b),
            "PostgreSQL nodes must converge on the same committed transaction state"
        );
    }

    if let Ok(path) = std::env::var("FERROSA_TEST_POSTGRES_HISTORY_PATH") {
        std::fs::write(path, serde_json::to_vec_pretty(&history)?)
            .context("write PostgreSQL Jepsen transaction history")?;
    }

    assert_eq!(
        final_a + final_b,
        2 * INITIAL_BALANCE,
        "committed transfers must preserve total balance"
    );
    check_strict_serializable(&initial, &final_state, &history)
        .map_err(anyhow::Error::from)
        .context("PostgreSQL transaction history violated strict serializability")?;

    clients[0]
        .batch_execute(&format!("DROP TABLE {table}"))
        .await
        .context("drop strict-serializability workload table")?;
    drop(clients);
    for connection in connections {
        connection
            .await
            .context("join PostgreSQL connection task")?
            .context("PostgreSQL connection task failed")?;
    }
    Ok(())
}

async fn connect_all(
    urls: &[&str],
) -> Result<(
    Vec<Arc<Client>>,
    Vec<tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>>,
)> {
    let mut clients = Vec::with_capacity(urls.len());
    let mut connections = Vec::with_capacity(urls.len());
    for (index, url) in urls.iter().enumerate() {
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .with_context(|| format!("connect to PostgreSQL node {index}"))?;
        connections.push(tokio::spawn(connection));
        clients.push(Arc::new(client));
    }
    Ok((clients, connections))
}

async fn wait_for_table_on_all_nodes(clients: &[Arc<Client>], table: &str) -> Result<()> {
    for (index, client) in clients.iter().enumerate().skip(1) {
        let mut last_error = None;
        for _ in 0..120 {
            match client
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .await
            {
                Ok(_) => {
                    last_error = None;
                    break;
                }
                Err(error) => {
                    last_error = Some(error);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
        if let Some(error) = last_error {
            return Err(error).with_context(|| {
                format!("PostgreSQL node {index} did not observe workload table {table}")
            });
        }
    }
    Ok(())
}

async fn read_balances(client: &Client, table: &str) -> Result<(i64, i64)> {
    let balance = |id| async move {
        Ok::<i64, tokio_postgres::Error>(
            client
                .query_one(
                    &format!("SELECT balance FROM {table} WHERE id = $1"),
                    &[&id],
                )
                .await?
                .get(0),
        )
    };
    Ok((balance("a").await?, balance("b").await?))
}

async fn transfer_once(
    client: &Client,
    table: &str,
    id: u64,
    event_clock: &AtomicU64,
) -> Result<RecordedTransaction> {
    let invoked = event_clock.fetch_add(1, Ordering::SeqCst);
    let mut operations = Vec::with_capacity(4);
    let transfer = async {
        client
            .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await?;

        let (source, destination) = if id.is_multiple_of(2) {
            ("a", "b")
        } else {
            ("b", "a")
        };
        let source_balance: i64 = client
            .query_one(
                &format!("SELECT balance FROM {table} WHERE id = $1"),
                &[&source],
            )
            .await?
            .get(0);
        operations.push(TransactionOperation::Read {
            key: source.to_owned(),
            value: Some(source_balance),
        });
        let destination_balance: i64 = client
            .query_one(
                &format!("SELECT balance FROM {table} WHERE id = $1"),
                &[&destination],
            )
            .await?
            .get(0);
        operations.push(TransactionOperation::Read {
            key: destination.to_owned(),
            value: Some(destination_balance),
        });

        if source_balance > 0 {
            let new_source = source_balance - 1;
            let new_destination = destination_balance + 1;
            let source_rows = client
                .execute(
                    &format!("UPDATE {table} SET balance = $1 WHERE id = $2"),
                    &[&new_source, &source],
                )
                .await?;
            assert_eq!(source_rows, 1, "source transfer row disappeared");
            operations.push(TransactionOperation::Write {
                key: source.to_owned(),
                value: Some(new_source),
            });
            let destination_rows = client
                .execute(
                    &format!("UPDATE {table} SET balance = $1 WHERE id = $2"),
                    &[&new_destination, &destination],
                )
                .await?;
            assert_eq!(destination_rows, 1, "destination transfer row disappeared");
            operations.push(TransactionOperation::Write {
                key: destination.to_owned(),
                value: Some(new_destination),
            });
        }
        client.batch_execute("COMMIT").await?;
        Result::<(), tokio_postgres::Error>::Ok(())
    }
    .await;

    let committed = match transfer {
        Ok(()) => true,
        Err(error) if is_serialization_failure(&error) => {
            client
                .batch_execute("ROLLBACK")
                .await
                .context("rollback PostgreSQL serialization failure")?;
            false
        }
        Err(error) => return Err(error).context("execute PostgreSQL transfer transaction"),
    };
    let completed = event_clock.fetch_add(1, Ordering::SeqCst);
    Ok(RecordedTransaction {
        id,
        invoked,
        completed,
        committed,
        operations,
    })
}

fn is_serialization_failure(error: &tokio_postgres::Error) -> bool {
    error
        .code()
        .is_some_and(|sqlstate| sqlstate.code() == "40001")
}
