//! Native PostgreSQL driver history test for distributed MVCC transactions.
//! Correctness: committed transactions must admit an atomic serial order that
//! reproduces reads and respects real-time precedence. Normal runs require every
//! node to converge; the pause schedule checks only the active quorum.
//! Last revised: 2026-09-27
//! Last changed: Added register, predicate/phantom, and write-skew histories.
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
use scylla::client::session_builder::SessionBuilder;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const ACTORS: usize = 5;
const TRANSACTIONS_PER_ACTOR: usize = 2;
// Bound retries so an unavailable cluster cannot hold a test actor indefinitely.
const TRANSFER_SERIALIZATION_RETRIES: usize = 8;
const WRITE_SKEW_WRITERS: usize = 2;
const WORKLOAD_STATE_READ_RETRIES: usize = 120;
const INITIAL_BALANCE: i64 = 10_000;
const POSTGRES_DEFAULT_SCHEMA: &str = "public";
const CLIENT_NODE_COUNT_ENV: &str = "FERROSA_TEST_POSTGRES_CLIENT_NODE_COUNT";
const CQL_URLS_ENV: &str = "FERROSA_TEST_CQL_URLS";
const FAULT_READY_FILE_ENV: &str = "FERROSA_TEST_POSTGRES_FAULT_READY_FILE";
const FAULT_ACTIVE_FILE_ENV: &str = "FERROSA_TEST_POSTGRES_FAULT_ACTIVE_FILE";
const FAULT_COMPLETE_FILE_ENV: &str = "FERROSA_TEST_POSTGRES_FAULT_COMPLETE_FILE";

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

    let actor_client_count = actor_client_count(
        urls.len(),
        std::env::var(CLIENT_NODE_COUNT_ENV).ok().as_deref(),
    )?;
    let cql_urls = std::env::var(CQL_URLS_ENV)
        .context("set FERROSA_TEST_CQL_URLS to semicolon-separated CQL node addresses")?;
    let cql_urls: Vec<_> = cql_urls
        .split(';')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(str::to_owned)
        .collect();
    if cql_urls.is_empty() {
        bail!("FERROSA_TEST_CQL_URLS must contain at least one CQL node address");
    }
    let fault_schedule = FaultSchedule::from_env()?;

    let (clients, connections) = connect_all(&urls).await?;
    let table = format!("pg_ssi_{}", Uuid::new_v4().simple());
    // PostgreSQL DDL is not part of this gateway's supported SQL surface. Create
    // the backing CQL table through CQL, then exercise only SQL DML/transactions
    // through the native PostgreSQL driver below.
    let cql_session = SessionBuilder::new()
        .known_nodes(&cql_urls)
        .build()
        .await
        .context("connect to CQL cluster for strict-serializability table setup")?;
    cql_session
        .query_unpaged(
            "CREATE KEYSPACE IF NOT EXISTS public WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}",
            &[],
        )
        .await
        .context("create PostgreSQL default schema keyspace through CQL")?;
    cql_session
        .query_unpaged(
            format!(
                "CREATE TABLE {POSTGRES_DEFAULT_SCHEMA}.{table} (id text PRIMARY KEY, balance bigint)"
            ),
            &[],
        )
        .await
        .context("create strict-serializability workload table through CQL")?;
    for statement in initial_workload_statements(&table) {
        clients[0]
            .execute(&statement, &[])
            .await
            .context("initialize strict-serializability workload balances")?;
    }
    wait_for_table_on_all_nodes(&clients, &table).await?;

    // Each Jepsen actor needs a distinct PostgreSQL session. Sharing one
    // Client per node lets concurrent actors interleave BEGIN/COMMIT on the
    // same server-side transaction state.
    let actor_urls = actor_client_urls(&urls, actor_client_count);
    let (actor_clients, actor_connections) = connect_all(&actor_urls).await?;

    if let Some(schedule) = &fault_schedule {
        schedule.announce_ready().await?;
        schedule.wait_until_injected().await?;
    }

    let event_clock = Arc::new(AtomicU64::new(1));
    let transfer_retry_count = Arc::new(AtomicUsize::new(0));
    let history = Arc::new(Mutex::new(Vec::with_capacity(
        ACTORS * (TRANSACTIONS_PER_ACTOR * 2 + 2),
    )));
    let predicate_barrier = Arc::new(tokio::sync::Barrier::new(ACTORS));
    let write_skew_barrier = Arc::new(tokio::sync::Barrier::new(ACTORS));
    let (actor_failure_tx, actor_failure_rx) = tokio::sync::watch::channel(None::<String>);
    let mut actors = Vec::with_capacity(ACTORS);
    for (actor, actor_client) in actor_clients.iter().enumerate() {
        let client = Arc::clone(actor_client);
        let event_clock = Arc::clone(&event_clock);
        let transfer_retry_count = Arc::clone(&transfer_retry_count);
        let history = Arc::clone(&history);
        let predicate_barrier = Arc::clone(&predicate_barrier);
        let write_skew_barrier = Arc::clone(&write_skew_barrier);
        let actor_failure_tx = actor_failure_tx.clone();
        let mut actor_failure_rx = actor_failure_rx.clone();
        let mut actor_cancel_rx = actor_failure_rx.clone();
        let table = table.clone();
        actors.push(tokio::spawn(async move {
            let result = run_until_actor_failure(
                async {
                    for iteration in 0..TRANSACTIONS_PER_ACTOR {
                        let operation_id = (actor * TRANSACTIONS_PER_ACTOR + iteration) as u64;
                        let transfers =
                            transfer_once(&client, &table, operation_id, &event_clock).await?;
                        transfer_retry_count
                            .fetch_add(transfers.len().saturating_sub(1), Ordering::Relaxed);
                        history
                            .lock()
                            .expect("history mutex poisoned")
                            .extend(transfers);
                        let register =
                            register_once(&client, &table, operation_id * 2 + 1, &event_clock)
                                .await?;
                        history
                            .lock()
                            .expect("history mutex poisoned")
                            .push(register);
                    }

                    wait_for_phase(&predicate_barrier, &mut actor_failure_rx).await?;
                    let predicate = if actor == 0 {
                        insert_phantom_once(&client, &table, 1_000, &event_clock).await?
                    } else {
                        observe_predicate_once(&client, &table, 1_000 + actor as u64, &event_clock)
                            .await?
                    };
                    history
                        .lock()
                        .expect("history mutex poisoned")
                        .push(predicate);

                    wait_for_phase(&write_skew_barrier, &mut actor_failure_rx).await?;
                    let write_skew = if is_write_skew_writer(actor) {
                        write_skew_once(&client, &table, 2_000 + actor as u64, &event_clock).await?
                    } else {
                        observe_predicate_once(&client, &table, 2_000 + actor as u64, &event_clock)
                            .await?
                    };
                    history
                        .lock()
                        .expect("history mutex poisoned")
                        .push(write_skew);
                    Ok(())
                },
                &mut actor_cancel_rx,
            )
            .await;

            if let Err(error) = &result {
                let failure = format!("PostgreSQL workload actor {actor} failed: {error:#}");
                actor_failure_tx.send_if_modified(|first_failure| {
                    if first_failure.is_none() {
                        *first_failure = Some(failure);
                        true
                    } else {
                        false
                    }
                });
            }
            result
        }));
    }
    let mut actor_error = None;
    for actor in actors {
        match actor.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if actor_error.is_none() => actor_error = Some(error),
            Err(error) if actor_error.is_none() => {
                actor_error =
                    Some(anyhow::Error::new(error).context("join PostgreSQL workload actor"))
            }
            Ok(Err(_)) | Err(_) => {}
        }
    }
    if let Some(schedule) = &fault_schedule {
        schedule.announce_complete().await?;
    }
    if let Some(error) = actor_error {
        return Err(error);
    }

    let final_state = read_workload_state_after_fault(&clients[0], &table).await?;
    let final_a = final_state["a"];
    let final_b = final_state["b"];
    let initial = BTreeMap::from([
        ("a".to_owned(), INITIAL_BALANCE),
        ("b".to_owned(), INITIAL_BALANCE),
        ("register".to_owned(), 0),
        ("doctor-a".to_owned(), 1),
        ("doctor-b".to_owned(), 1),
    ]);
    let mut history = history.lock().expect("history mutex poisoned").clone();
    history.sort_by_key(|transaction| transaction.id);
    assert_eq!(
        history.len(),
        ACTORS * (TRANSACTIONS_PER_ACTOR * 2 + 2) + transfer_retry_count.load(Ordering::Relaxed),
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
    assert!(
        history.iter().any(|transaction| {
            transaction.committed
                && transaction.operations.iter().any(|operation| {
                    matches!(operation, TransactionOperation::Write { key, .. } if key == "register")
                })
        }),
        "the native register workload must commit at least one read/modify/write"
    );
    assert!(
        history
            .iter()
            .filter(|transaction| {
                transaction.committed
                    && transaction.operations.iter().any(|operation| {
                        matches!(operation, TransactionOperation::Write { key, .. } if key == "a" || key == "b")
                    })
            })
            .count()
            >= 2,
        "the native transfer workload must commit multiple multi-row transactions"
    );
    assert!(
        history.iter().any(|transaction| {
            transaction.operations.iter().any(|operation| {
                matches!(operation, TransactionOperation::Write { key, .. } if key == "phantom")
            })
        }),
        "the predicate workload must attempt a phantom insert"
    );
    assert!(
        history.iter().any(|transaction| {
            transaction.committed
                && transaction.operations.iter().any(|operation| {
                    matches!(operation, TransactionOperation::Write { key, .. } if key.starts_with("doctor-"))
                })
        }),
        "the native write-skew workload must commit at least one doctor update"
    );

    // The fault schedule verifies transaction history and agreement on the
    // reachable quorum. Rejoining a replica's Accord catch-up is a separate
    // recovery contract; normal runs still require every node to converge.
    let convergence_nodes =
        convergence_node_count(clients.len(), actor_client_count, fault_schedule.is_some());
    for (node, client) in clients.iter().enumerate().take(convergence_nodes).skip(1) {
        wait_for_workload_state(client, &table, &final_state)
            .await
            .with_context(|| {
                format!(
                    "PostgreSQL node {node} did not converge on the committed transaction state"
                )
            })?;
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
    assert_eq!(
        final_state["doctor-a"] + final_state["doctor-b"],
        1,
        "serializable write-skew workload must preserve one doctor on call"
    );
    check_strict_serializable(&initial, &final_state, &history)
        .map_err(anyhow::Error::from)
        .context("PostgreSQL transaction history violated strict serializability")?;

    cql_session
        .query_unpaged(format!("DROP TABLE {POSTGRES_DEFAULT_SCHEMA}.{table}"), &[])
        .await
        .context("drop strict-serializability workload table through CQL")?;
    drop(clients);
    drop(actor_clients);
    for connection in connections.into_iter().chain(actor_connections) {
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

async fn read_workload_state(client: &Client, table: &str) -> Result<BTreeMap<String, i64>> {
    let mut state = BTreeMap::new();
    for id in ["a", "b", "register", "doctor-a", "doctor-b", "phantom"] {
        if let Some(row) = client
            .query_opt(
                &format!("SELECT balance FROM {table} WHERE id = $1"),
                &[&id],
            )
            .await?
        {
            state.insert(id.to_owned(), row.get(0));
        }
    }
    Ok(state)
}

async fn read_workload_state_after_fault(
    client: &Client,
    table: &str,
) -> Result<BTreeMap<String, i64>> {
    let mut last_error = None;
    for attempt in 0..=WORKLOAD_STATE_READ_RETRIES {
        match read_workload_state(client, table).await {
            Ok(state) => return Ok(state),
            Err(error)
                if error
                    .downcast_ref::<tokio_postgres::Error>()
                    .is_some_and(is_serialization_failure) =>
            {
                last_error = Some(error);
                if attempt < WORKLOAD_STATE_READ_RETRIES {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.expect("retry loop records a serialization failure"))
        .context("read workload state after the replica fault")
}

async fn wait_for_workload_state(
    client: &Client,
    table: &str,
    expected: &BTreeMap<String, i64>,
) -> Result<()> {
    let mut last_state = None;
    let mut last_error = None;
    for _ in 0..120 {
        match read_workload_state(client, table).await {
            Ok(state) if &state == expected => return Ok(()),
            Ok(state) => {
                last_state = Some(state);
                last_error = None;
            }
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    if let Some(error) = last_error {
        return Err(error).context("read workload state while waiting for replica convergence");
    }
    bail!("replica state {last_state:?} did not converge to {expected:?}");
}

async fn transfer_once(
    client: &Client,
    table: &str,
    id: u64,
    event_clock: &AtomicU64,
) -> Result<Vec<RecordedTransaction>> {
    retry_serializable_transactions(|| transfer_attempt(client, table, id, event_clock)).await
}

async fn transfer_attempt(
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

    finish_recorded_transaction(
        client,
        invoked,
        operations,
        transfer,
        event_clock,
        "execute PostgreSQL transfer transaction",
    )
    .await
}

async fn register_once(
    client: &Client,
    table: &str,
    _id: u64,
    event_clock: &AtomicU64,
) -> Result<RecordedTransaction> {
    let invoked = event_clock.fetch_add(1, Ordering::SeqCst);
    let mut operations = Vec::with_capacity(2);
    let transaction = async {
        client
            .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await?;
        let value: i64 = client
            .query_one(
                &format!("SELECT balance FROM {table} WHERE id = $1"),
                &[&"register"],
            )
            .await?
            .get(0);
        operations.push(TransactionOperation::Read {
            key: "register".to_owned(),
            value: Some(value),
        });
        let next = value + 1;
        client
            .execute(
                &format!("UPDATE {table} SET balance = $1 WHERE id = $2"),
                &[&next, &"register"],
            )
            .await?;
        operations.push(TransactionOperation::Write {
            key: "register".to_owned(),
            value: Some(next),
        });
        client.batch_execute("COMMIT").await?;
        Result::<(), tokio_postgres::Error>::Ok(())
    }
    .await;

    finish_recorded_transaction(
        client,
        invoked,
        operations,
        transaction,
        event_clock,
        "execute PostgreSQL register transaction",
    )
    .await
}

async fn observe_predicate_once(
    client: &Client,
    table: &str,
    _id: u64,
    event_clock: &AtomicU64,
) -> Result<RecordedTransaction> {
    let invoked = event_clock.fetch_add(1, Ordering::SeqCst);
    let mut operations = Vec::with_capacity(3);
    let transaction = async {
        client
            .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await?;
        let observed = read_predicate_values(client, table).await?;
        operations.extend(predicate_observations(&observed));
        client.batch_execute("COMMIT").await?;
        Result::<(), tokio_postgres::Error>::Ok(())
    }
    .await;

    finish_recorded_transaction(
        client,
        invoked,
        operations,
        transaction,
        event_clock,
        "execute PostgreSQL predicate-read transaction",
    )
    .await
}

async fn insert_phantom_once(
    client: &Client,
    table: &str,
    _id: u64,
    event_clock: &AtomicU64,
) -> Result<RecordedTransaction> {
    let invoked = event_clock.fetch_add(1, Ordering::SeqCst);
    let mut operations = Vec::with_capacity(4);
    let transaction = async {
        client
            .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await?;
        let observed = read_predicate_values(client, table).await?;
        operations.extend(predicate_observations(&observed));
        let phantom_value = 1_i64;
        client
            .execute(
                &format!("INSERT INTO {table} (id, balance) VALUES ($1, $2)"),
                &[&"phantom", &phantom_value],
            )
            .await?;
        operations.push(TransactionOperation::Write {
            key: "phantom".to_owned(),
            value: Some(phantom_value),
        });
        client.batch_execute("COMMIT").await?;
        Result::<(), tokio_postgres::Error>::Ok(())
    }
    .await;

    finish_recorded_transaction(
        client,
        invoked,
        operations,
        transaction,
        event_clock,
        "execute PostgreSQL predicate phantom insert",
    )
    .await
}

async fn write_skew_once(
    client: &Client,
    table: &str,
    id: u64,
    event_clock: &AtomicU64,
) -> Result<RecordedTransaction> {
    let invoked = event_clock.fetch_add(1, Ordering::SeqCst);
    let mut operations = Vec::with_capacity(4);
    let transaction = async {
        client
            .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await?;
        let observed = read_predicate_values(client, table).await?;
        operations.extend(predicate_observations(&observed));
        if observed.get("doctor-a") == Some(&Some(1)) && observed.get("doctor-b") == Some(&Some(1))
        {
            let target = if id.is_multiple_of(2) {
                "doctor-a"
            } else {
                "doctor-b"
            };
            let off_call = 0_i64;
            client
                .execute(
                    &format!("UPDATE {table} SET balance = $1 WHERE id = $2"),
                    &[&off_call, &target],
                )
                .await?;
            operations.push(TransactionOperation::Write {
                key: target.to_owned(),
                value: Some(off_call),
            });
        }
        client.batch_execute("COMMIT").await?;
        Result::<(), tokio_postgres::Error>::Ok(())
    }
    .await;

    finish_recorded_transaction(
        client,
        invoked,
        operations,
        transaction,
        event_clock,
        "execute PostgreSQL write-skew transaction",
    )
    .await
}

async fn read_predicate_values(
    client: &Client,
    table: &str,
) -> Result<BTreeMap<String, Option<i64>>, tokio_postgres::Error> {
    let mut observed = BTreeMap::new();
    for key in ["doctor-a", "doctor-b", "phantom"] {
        let value = client
            .query_opt(
                &format!("SELECT balance FROM {table} WHERE id = $1"),
                &[&key],
            )
            .await?
            .map(|row| row.get(0));
        observed.insert(key.to_owned(), value);
    }
    Ok(observed)
}

async fn finish_recorded_transaction(
    client: &Client,
    invoked: u64,
    operations: Vec<TransactionOperation>,
    result: Result<(), tokio_postgres::Error>,
    event_clock: &AtomicU64,
    failure_context: &'static str,
) -> Result<RecordedTransaction> {
    let committed = match result {
        Ok(()) => true,
        Err(error) if is_serialization_failure(&error) => {
            client
                .batch_execute("ROLLBACK")
                .await
                .context("rollback PostgreSQL serialization failure")?;
            false
        }
        Err(error) => return Err(error).context(failure_context),
    };
    let completed = event_clock.fetch_add(1, Ordering::SeqCst);
    Ok(RecordedTransaction {
        id: invoked,
        invoked,
        completed,
        committed,
        operations,
    })
}

async fn retry_serializable_transactions<F, Fut>(mut attempt: F) -> Result<Vec<RecordedTransaction>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<RecordedTransaction>>,
{
    let mut history = Vec::with_capacity(TRANSFER_SERIALIZATION_RETRIES + 1);
    for _ in 0..=TRANSFER_SERIALIZATION_RETRIES {
        let transaction = attempt().await?;
        let committed = transaction.committed;
        history.push(transaction);
        if committed {
            break;
        }
        tokio::task::yield_now().await;
    }
    Ok(history)
}

async fn wait_for_phase(
    barrier: &tokio::sync::Barrier,
    actor_failure_rx: &mut tokio::sync::watch::Receiver<Option<String>>,
) -> Result<()> {
    loop {
        if let Some(error) = actor_failure_rx.borrow().as_ref() {
            bail!("{error}");
        }

        tokio::select! {
            _ = barrier.wait() => return Ok(()),
            changed = actor_failure_rx.changed() => {
                if changed.is_err() {
                    bail!("PostgreSQL workload actor failure signal closed at the phase barrier");
                }
                if let Some(error) = actor_failure_rx.borrow().as_ref() {
                    bail!("{error}");
                }
            }
        }
    }
}

async fn run_until_actor_failure<F>(
    work: F,
    actor_failure_rx: &mut tokio::sync::watch::Receiver<Option<String>>,
) -> Result<()>
where
    F: std::future::Future<Output = Result<()>>,
{
    if let Some(error) = actor_failure_rx.borrow().clone() {
        bail!("{error}");
    }

    tokio::select! {
        result = work => result,
        changed = actor_failure_rx.changed() => {
            if changed.is_err() {
                bail!("PostgreSQL workload actor failure signal closed during actor work");
            }
            if let Some(error) = actor_failure_rx.borrow().as_ref() {
                bail!("{error}");
            }
            bail!("PostgreSQL workload actor failure signal changed without an error");
        }
    }
}

fn is_serialization_failure(error: &tokio_postgres::Error) -> bool {
    error
        .code()
        .is_some_and(|sqlstate| is_retryable_serialization_state(sqlstate.code()))
}

fn is_retryable_serialization_state(sqlstate: &str) -> bool {
    sqlstate == "40001"
}

fn initial_workload_statements(table: &str) -> [String; 5] {
    [
        format!("INSERT INTO {table} (id, balance) VALUES ('a', {INITIAL_BALANCE})"),
        format!("INSERT INTO {table} (id, balance) VALUES ('b', {INITIAL_BALANCE})"),
        format!("INSERT INTO {table} (id, balance) VALUES ('register', 0)"),
        format!("INSERT INTO {table} (id, balance) VALUES ('doctor-a', 1)"),
        format!("INSERT INTO {table} (id, balance) VALUES ('doctor-b', 1)"),
    ]
}

fn predicate_observations(observed: &BTreeMap<String, Option<i64>>) -> Vec<TransactionOperation> {
    ["doctor-a", "doctor-b", "phantom"]
        .into_iter()
        .map(|key| TransactionOperation::Read {
            key: key.to_owned(),
            value: observed.get(key).copied().flatten(),
        })
        .collect()
}

/// Selects the client endpoints used for transactions while still retaining
/// every endpoint for convergence checks. The fault workflow excludes the
/// node it pauses; normal runs use every supplied endpoint.
fn actor_client_count(url_count: usize, configured: Option<&str>) -> Result<usize> {
    let count = configured
        .map(str::parse::<usize>)
        .transpose()
        .context("parse FERROSA_TEST_POSTGRES_CLIENT_NODE_COUNT")?
        .unwrap_or(url_count);
    if count < 2 || count > url_count {
        bail!(
            "{CLIENT_NODE_COUNT_ENV} must be between 2 and the number of PostgreSQL URLs ({url_count}), got {count}"
        );
    }
    Ok(count)
}

/// Assign one session endpoint per actor, spreading sessions across the
/// selected nodes without sharing a PostgreSQL transaction state machine.
fn actor_client_urls<'a>(urls: &'a [&'a str], node_count: usize) -> Vec<&'a str> {
    (0..ACTORS).map(|actor| urls[actor % node_count]).collect()
}

fn is_write_skew_writer(actor: usize) -> bool {
    actor < WRITE_SKEW_WRITERS
}

fn convergence_node_count(total_nodes: usize, active_nodes: usize, fault_scheduled: bool) -> usize {
    if fault_scheduled {
        active_nodes
    } else {
        total_nodes
    }
}

/// Coordinates a real node failure with an external process controller. The
/// test reports that schema setup is complete, waits until the controller has
/// paused a replica, runs the workload, then signals that the replica can be
/// resumed before convergence checks.
struct FaultSchedule {
    ready_file: PathBuf,
    active_file: PathBuf,
    complete_file: PathBuf,
}

impl FaultSchedule {
    fn from_env() -> Result<Option<Self>> {
        Self::from_paths(
            std::env::var_os(FAULT_READY_FILE_ENV).map(PathBuf::from),
            std::env::var_os(FAULT_ACTIVE_FILE_ENV).map(PathBuf::from),
            std::env::var_os(FAULT_COMPLETE_FILE_ENV).map(PathBuf::from),
        )
    }

    fn from_paths(
        ready_file: Option<PathBuf>,
        active_file: Option<PathBuf>,
        complete_file: Option<PathBuf>,
    ) -> Result<Option<Self>> {
        let configured = [ready_file, active_file, complete_file];
        if configured.iter().all(Option::is_none) {
            return Ok(None);
        }
        if configured.iter().any(Option::is_none) {
            bail!("fault schedule requires all three PostgreSQL fault marker paths");
        }
        let [Some(ready_file), Some(active_file), Some(complete_file)] = configured else {
            unreachable!("all PostgreSQL fault marker paths were checked above")
        };
        Ok(Some(Self {
            ready_file,
            active_file,
            complete_file,
        }))
    }

    async fn announce_ready(&self) -> Result<()> {
        write_marker(&self.ready_file).await
    }

    async fn wait_until_injected(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(120), async {
            while tokio::fs::metadata(&self.active_file).await.is_err() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("timed out waiting for the PostgreSQL replica pause")
    }

    async fn announce_complete(&self) -> Result<()> {
        write_marker(&self.complete_file).await
    }
}

async fn write_marker(path: &PathBuf) -> Result<()> {
    tokio::fs::write(path, [])
        .await
        .with_context(|| format!("write PostgreSQL fault marker {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{
        actor_client_count, actor_client_urls, convergence_node_count, initial_workload_statements,
        is_retryable_serialization_state, is_write_skew_writer, predicate_observations,
        retry_serializable_transactions, run_until_actor_failure, wait_for_phase, FaultSchedule,
        RecordedTransaction, TransactionOperation, ACTORS, TRANSFER_SERIALIZATION_RETRIES,
    };

    #[tokio::test]
    async fn serialization_retries_keep_aborted_attempts_and_stop_after_commit() {
        let mut outcomes = [false, false, true].into_iter();
        let history = retry_serializable_transactions(|| {
            let committed = outcomes.next().expect("only three attempts expected");
            async move {
                Ok(RecordedTransaction {
                    id: 0,
                    invoked: 0,
                    completed: 1,
                    committed,
                    operations: Vec::new(),
                })
            }
        })
        .await
        .expect("serialization failures are recorded, not returned as errors");

        assert_eq!(
            history
                .iter()
                .map(|transaction| transaction.committed)
                .collect::<Vec<_>>(),
            [false, false, true]
        );
        assert!(
            outcomes.next().is_none(),
            "successful commit must stop retries"
        );
    }

    #[tokio::test]
    async fn serialization_retries_are_bounded_and_keep_every_abort() {
        let attempts = std::cell::Cell::new(0);
        let history = retry_serializable_transactions(|| {
            attempts.set(attempts.get() + 1);
            async {
                Ok(RecordedTransaction {
                    id: 0,
                    invoked: 0,
                    completed: 1,
                    committed: false,
                    operations: Vec::new(),
                })
            }
        })
        .await
        .expect("serialization failures are retained as aborted attempts");

        assert_eq!(attempts.get(), TRANSFER_SERIALIZATION_RETRIES + 1);
        assert_eq!(history.len(), TRANSFER_SERIALIZATION_RETRIES + 1);
        assert!(history.iter().all(|transaction| !transaction.committed));
    }

    #[test]
    fn workload_fixture_uses_single_row_inserts_supported_by_the_postgres_gateway() {
        let statements = initial_workload_statements("accounts");

        assert_eq!(statements.len(), 5);
        assert_eq!(
            statements[0],
            "INSERT INTO accounts (id, balance) VALUES ('a', 10000)"
        );
        assert_eq!(
            statements[1],
            "INSERT INTO accounts (id, balance) VALUES ('b', 10000)"
        );
    }

    #[test]
    fn workload_fixture_includes_register_and_write_skew_rows() {
        let statements = initial_workload_statements("accounts");

        assert!(statements
            .iter()
            .any(|statement| statement.contains("VALUES ('register', 0)")));
        assert!(statements
            .iter()
            .any(|statement| statement.contains("VALUES ('doctor-a', 1)")));
        assert!(statements
            .iter()
            .any(|statement| statement.contains("VALUES ('doctor-b', 1)")));
    }

    #[test]
    fn fault_workload_limits_write_skew_to_two_competing_actors() {
        let writers = (0..ACTORS)
            .filter(|actor| is_write_skew_writer(*actor))
            .collect::<Vec<_>>();

        assert_eq!(writers, [0, 1]);
        assert!(!is_write_skew_writer(ACTORS));
    }

    #[test]
    fn predicate_observation_records_known_values_and_absent_phantom() {
        let observed = std::collections::BTreeMap::from([
            ("doctor-a".to_owned(), Some(1)),
            ("doctor-b".to_owned(), Some(0)),
            ("phantom".to_owned(), None),
        ]);

        assert_eq!(
            predicate_observations(&observed),
            [
                TransactionOperation::Read {
                    key: "doctor-a".to_owned(),
                    value: Some(1),
                },
                TransactionOperation::Read {
                    key: "doctor-b".to_owned(),
                    value: Some(0),
                },
                TransactionOperation::Read {
                    key: "phantom".to_owned(),
                    value: None,
                },
            ]
        );
    }

    #[test]
    fn only_serialization_failures_are_retryable_for_final_state_reads() {
        assert!(is_retryable_serialization_state("40001"));
        assert!(!is_retryable_serialization_state("23505"));
        assert!(!is_retryable_serialization_state("08006"));
    }

    #[tokio::test]
    async fn actor_failure_releases_peers_waiting_at_a_workload_phase() {
        let barrier = tokio::sync::Barrier::new(2);
        let (failure_tx, mut failure_rx) = tokio::sync::watch::channel(None::<String>);
        let waiting_actor =
            tokio::spawn(async move { wait_for_phase(&barrier, &mut failure_rx).await });

        failure_tx.send_replace(Some(
            "PostgreSQL workload actor 2 failed: execute PostgreSQL register transaction: connection reset"
                .to_owned(),
        ));
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), waiting_actor)
            .await
            .expect("failed actor should release its peers from the barrier")
            .expect("waiting actor task should not panic");

        let error = result.expect_err("peer should receive the actor failure");
        assert!(error.to_string().contains("actor 2"));
        assert!(error.to_string().contains("connection reset"));
    }

    #[tokio::test]
    async fn actor_failure_cancels_peer_work_and_preserves_originating_error() {
        let (failure_tx, mut failure_rx) = tokio::sync::watch::channel(None::<String>);
        let peer = tokio::spawn(async move {
            run_until_actor_failure(std::future::pending(), &mut failure_rx).await
        });

        failure_tx.send_replace(Some(
            "PostgreSQL workload actor 2 failed: execute PostgreSQL register transaction: connection reset"
                .to_owned(),
        ));
        let error = tokio::time::timeout(std::time::Duration::from_millis(100), peer)
            .await
            .expect("actor failure should cancel peer database work")
            .expect("peer task should not panic")
            .expect_err("peer should receive the originating actor failure");

        assert!(error.to_string().contains("actor 2"));
        assert!(error.to_string().contains("connection reset"));
    }

    #[tokio::test]
    async fn actor_failure_already_signaled_cancels_late_peer_work() {
        let (failure_tx, mut failure_rx) = tokio::sync::watch::channel(None::<String>);
        failure_tx.send_replace(Some("PostgreSQL workload actor 1 failed".to_owned()));

        let error = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            run_until_actor_failure(std::future::pending(), &mut failure_rx),
        )
        .await
        .expect("late actors must observe an already signaled peer failure")
        .expect_err("peer should receive the prior actor failure");

        assert!(error.to_string().contains("actor 1"));
    }

    #[test]
    fn fault_workload_can_leave_one_of_three_nodes_unused_by_clients() {
        assert_eq!(actor_client_count(3, Some("2")).unwrap(), 2);
    }

    #[test]
    fn fault_workload_gives_each_actor_a_separate_session_on_active_nodes() {
        let nodes = ["node1", "node2", "paused-node"];

        assert_eq!(
            actor_client_urls(&nodes, 2),
            ["node1", "node2", "node1", "node2", "node1"]
        );
    }

    #[test]
    fn fault_workload_checks_convergence_only_on_active_nodes() {
        assert_eq!(convergence_node_count(3, 2, true), 2);
        assert_eq!(convergence_node_count(3, 3, false), 3);
    }

    #[test]
    fn default_workload_uses_every_postgres_node() {
        assert_eq!(actor_client_count(3, None).unwrap(), 3);
    }

    #[test]
    fn fault_workload_rejects_fewer_than_two_client_nodes() {
        assert!(actor_client_count(3, Some("1")).is_err());
    }

    #[test]
    fn fault_workload_rejects_client_count_above_url_count() {
        assert!(actor_client_count(3, Some("4")).is_err());
    }

    #[test]
    fn fault_workload_rejects_non_numeric_client_count() {
        assert!(actor_client_count(3, Some("two")).is_err());
    }

    #[test]
    fn fault_schedule_requires_ready_active_and_complete_markers_together() {
        assert!(
            FaultSchedule::from_paths(Some("ready".into()), None, Some("complete".into())).is_err()
        );
    }

    #[test]
    fn fault_schedule_is_disabled_when_no_marker_paths_are_configured() {
        assert!(FaultSchedule::from_paths(None, None, None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn fault_schedule_accepts_all_three_marker_paths() {
        assert!(FaultSchedule::from_paths(
            Some("ready".into()),
            Some("active".into()),
            Some("complete".into())
        )
        .unwrap()
        .is_some());
    }
}
