//! Native PostgreSQL driver history test for distributed MVCC transactions.
//! Correctness: Completed committed transactions must admit a serial order that
//! respects real-time precedence, and all nodes must converge on the final state.
//! Last revised: 2026-09-26
//! Last changed: Added a coordinated one-replica pause schedule for the native-driver history.
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const ACTORS: usize = 5;
const TRANSACTIONS_PER_ACTOR: usize = 4;
const INITIAL_BALANCE: i64 = 10_000;
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
            format!("CREATE TABLE postgres.{table} (id text PRIMARY KEY, balance bigint)"),
            &[],
        )
        .await
        .context("create strict-serializability workload table through CQL")?;
    for statement in initial_balance_statements(&table) {
        clients[0]
            .execute(&statement, &[])
            .await
            .context("initialize strict-serializability workload balances")?;
    }
    wait_for_table_on_all_nodes(&clients, &table).await?;

    if let Some(schedule) = &fault_schedule {
        schedule.announce_ready().await?;
        schedule.wait_until_injected().await?;
    }

    let event_clock = Arc::new(AtomicU64::new(1));
    let history = Arc::new(Mutex::new(Vec::with_capacity(
        ACTORS * TRANSACTIONS_PER_ACTOR,
    )));
    let mut actors = Vec::with_capacity(ACTORS);
    for actor in 0..ACTORS {
        let client = Arc::clone(&clients[actor % actor_client_count]);
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
        wait_for_balances(client, &table, (final_a, final_b))
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
    check_strict_serializable(&initial, &final_state, &history)
        .map_err(anyhow::Error::from)
        .context("PostgreSQL transaction history violated strict serializability")?;

    cql_session
        .query_unpaged(format!("DROP TABLE postgres.{table}"), &[])
        .await
        .context("drop strict-serializability workload table through CQL")?;
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

async fn wait_for_balances(client: &Client, table: &str, expected: (i64, i64)) -> Result<()> {
    let mut last_state = None;
    let mut last_error = None;
    for _ in 0..120 {
        match read_balances(client, table).await {
            Ok(state) if state == expected => return Ok(()),
            Ok(state) => {
                last_state = Some(state);
                last_error = None;
            }
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    if let Some(error) = last_error {
        return Err(error).context("read balances while waiting for replica convergence");
    }
    bail!("replica state {last_state:?} did not converge to {expected:?}");
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

fn initial_balance_statements(table: &str) -> [String; 2] {
    [
        format!("INSERT INTO {table} (id, balance) VALUES ('a', {INITIAL_BALANCE})"),
        format!("INSERT INTO {table} (id, balance) VALUES ('b', {INITIAL_BALANCE})"),
    ]
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
    use super::{actor_client_count, initial_balance_statements, FaultSchedule};

    #[test]
    fn balance_fixture_uses_single_row_inserts_supported_by_the_postgres_gateway() {
        let statements = initial_balance_statements("accounts");

        assert_eq!(statements.len(), 2);
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
    fn fault_workload_can_leave_one_of_three_nodes_unused_by_clients() {
        assert_eq!(actor_client_count(3, Some("2")).unwrap(), 2);
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
