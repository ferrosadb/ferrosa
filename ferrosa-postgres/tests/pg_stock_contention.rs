//! t_3c15a13f: Direct READ COMMITTED contention. No pgbench executable is needed.
//! Test list: stock TPC-B committed-history conservation; default hot-row
//! wait/re-evaluation; explicit READ COMMITTED per-statement visibility.
//! Existing explicit SERIALIZABLE conflict tests remain unchanged.

#[path = "common/pg_server.rs"]
mod pg_server;

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_postgres::{Client, Config, Error, NoTls, SimpleQueryMessage};

const HANG_GUARD: Duration = Duration::from_secs(30);
const OVERLAP_GATE: Duration = Duration::from_millis(250);

#[derive(Default)]
struct Wire {
    incoming: Vec<u8>,
    tags: Vec<String>,
    started: Option<oneshot::Sender<()>>,
    written: bool,
}

// Observe actual driver I/O, not merely spawning a task. Capture backend tags
// because tokio-postgres's SimpleQueryMessage erases COMMIT vs ROLLBACK.
struct ObservedSocket {
    socket: TcpStream,
    wire: Arc<Mutex<Wire>>,
}

impl AsyncRead for ObservedSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.socket).poll_read(cx, buf);
        let mut wire = self.wire.lock().unwrap();
        wire.incoming.extend_from_slice(&buf.filled()[before..]);
        while wire.incoming.len() >= 5 {
            let length = u32::from_be_bytes(wire.incoming[1..5].try_into().unwrap()) as usize;
            assert!(length >= 4, "invalid backend frame length");
            if wire.incoming.len() < length + 1 {
                break;
            }
            if wire.incoming[0] == b'C' {
                let tag = String::from_utf8(wire.incoming[5..length].to_vec()).unwrap();
                wire.tags.push(tag);
            }
            wire.incoming.drain(..length + 1);
        }
        result
    }
}

impl AsyncWrite for ObservedSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.socket).poll_write(cx, bytes);
        if matches!(result, Poll::Ready(Ok(n)) if n > 0) {
            self.wire.lock().unwrap().written = true;
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.socket).poll_flush(cx);
        if matches!(result, Poll::Ready(Ok(()))) {
            let mut wire = self.wire.lock().unwrap();
            if wire.written {
                if let Some(started) = wire.started.take() {
                    let _ = started.send(());
                }
                wire.written = false;
            }
        }
        result
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

struct Session {
    client: Client,
    wire: Arc<Mutex<Wire>>,
    driver: tokio::task::JoinHandle<()>,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

impl Session {
    async fn connect(port: u16) -> Self {
        let wire = Arc::new(Mutex::new(Wire::default()));
        let socket = ObservedSocket {
            socket: TcpStream::connect(("127.0.0.1", port)).await.unwrap(),
            wire: Arc::clone(&wire),
        };
        let (client, connection) = Config::new()
            .user("ferrosa_user")
            .password("devpass")
            .dbname("ferrosa")
            .connect_raw(socket, NoTls)
            .await
            .expect("fixture authentication");
        let driver = tokio::spawn(async move { connection.await.expect("wire connection") });
        Self {
            client,
            wire,
            driver,
        }
    }

    fn arm_request(&self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let mut wire = self.wire.lock().unwrap();
        wire.written = false;
        wire.started = Some(tx);
        rx
    }

    async fn end(&self, sql: &str) -> Result<(), Error> {
        self.wire.lock().unwrap().tags.clear();
        self.client.simple_query(sql).await?;
        assert_eq!(
            self.wire.lock().unwrap().tags,
            [sql],
            "{sql} must have its own command tag; a ROLLBACK is never a committed transaction"
        );
        Ok(())
    }
}

async fn bounded(work: impl Future<Output = ()>) {
    tokio::time::timeout(HANG_GUARD, work)
        .await
        .expect("stock contention regression exceeded its hang guard");
}

async fn sql(client: &Client, statement: &str) {
    client
        .simple_query(statement)
        .await
        .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
}

async fn rows(client: &Client, statement: &str) -> Vec<Vec<String>> {
    client
        .simple_query(statement)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).expect("non-NULL fixture column").to_string())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

async fn seed(client: &Client) {
    // Stock pgbench relations and DML, reduced to one hot row per relation.
    // No history PK: every successful transaction must append its own row.
    for statement in [
        "CREATE TABLE pgbench_branches (bid integer PRIMARY KEY, bbalance integer, filler char(88))",
        "CREATE TABLE pgbench_tellers (tid integer PRIMARY KEY, bid integer, tbalance integer, filler char(84))",
        "CREATE TABLE pgbench_accounts (aid integer PRIMARY KEY, bid integer, abalance integer, filler char(84))",
        "CREATE TABLE pgbench_history (tid integer, bid integer, aid integer, delta integer, mtime timestamp, filler char(22))",
        "INSERT INTO pgbench_branches (bid, bbalance) VALUES (1, 0)",
        "INSERT INTO pgbench_tellers (tid, bid, tbalance) VALUES (1, 1, 0)",
        "INSERT INTO pgbench_accounts (aid, bid, abalance) VALUES (1, 1, 0)",
    ] {
        sql(client, statement).await;
    }
}

async fn update(client: &Client, statement: &str) -> Result<(), Error> {
    let messages = client.simple_query(statement).await?;
    let counts: Vec<_> = messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::CommandComplete(count) => Some(*count),
            _ => None,
        })
        .collect();
    assert_eq!(counts, [1], "exactly one row must change: {statement}");
    Ok(())
}

async fn stock_body(client: &Client, delta: i32) -> Result<(), Error> {
    update(
        client,
        &format!("UPDATE pgbench_accounts SET abalance = abalance + {delta} WHERE aid = 1"),
    )
    .await?;
    let balance = client
        .simple_query("SELECT abalance FROM pgbench_accounts WHERE aid = 1")
        .await?;
    assert_eq!(
        balance
            .iter()
            .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
            .count(),
        1
    );
    update(
        client,
        &format!("UPDATE pgbench_tellers SET tbalance = tbalance + {delta} WHERE tid = 1"),
    )
    .await?;
    update(
        client,
        &format!("UPDATE pgbench_branches SET bbalance = bbalance + {delta} WHERE bid = 1"),
    )
    .await?;
    update(client, &format!(
        "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (1, 1, 1, {delta}, CURRENT_TIMESTAMP)"
    )).await
}

// A completed second request establishes overlap directly. Otherwise the
// bounded gate releases the first writer after an actual wire flush, allowing
// a row-lock implementation to proceed. Duration alone is NOT failure evidence.
// Pending cannot prove server lock admission: a stronger future seam would
// acknowledge arrival at row-lock acquisition, before waiting or evaluating RHS.
async fn overlap<T>(
    started: oneshot::Receiver<()>,
    task: &mut tokio::task::JoinHandle<T>,
) -> Option<T> {
    started.await.expect("second request reached wire flush");
    match tokio::time::timeout(OVERLAP_GATE, task).await {
        Ok(result) => {
            eprintln!("second operation completed while first transaction was open");
            Some(result.unwrap())
        }
        Err(_) => {
            eprintln!("second request flushed; bounded gate releasing first transaction");
            None
        }
    }
}

async fn conservation(client: &Client, committed: &[i32]) {
    let history = rows(
        client,
        "SELECT tid, bid, aid, delta, mtime FROM pgbench_history",
    )
    .await;
    let mut deltas = Vec::new();
    for row in &history {
        assert_eq!(&row[..3], ["1", "1", "1"]);
        assert!(
            !row[4].is_empty(),
            "stock history timestamp must be present"
        );
        deltas.push(row[3].parse::<i32>().unwrap());
    }
    let mut expected = committed.to_vec();
    expected.sort_unstable();
    deltas.sort_unstable();
    assert_eq!(
        deltas, expected,
        "only successful COMMITs belong in history"
    );
    assert_eq!(history.len(), committed.len());
    let sum: i32 = committed.iter().sum();
    for (table, id, balance) in [
        ("pgbench_accounts", "aid", "abalance"),
        ("pgbench_tellers", "tid", "tbalance"),
        ("pgbench_branches", "bid", "bbalance"),
    ] {
        let actual = rows(client, &format!("SELECT {id}, {balance} FROM {table}")).await;
        assert_eq!(
            actual,
            vec![vec!["1".to_string(), sum.to_string()]],
            "{table}: row count and sum must equal committed history; silent lost update"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_stock_tpcb_conserves_successfully_committed_history() {
    bounded(async {
        let server = pg_server::start_server_with_ddl().await;
        let first = Session::connect(server.port).await;
        let second = Session::connect(server.port).await;
        seed(&first.client).await;
        sql(&first.client, "BEGIN").await;
        sql(&second.client, "BEGIN").await;
        stock_body(&first.client, 7)
            .await
            .expect("first stock transaction");
        let started = second.arm_request();
        let mut task = tokio::spawn(async move {
            let body = stock_body(&second.client, 11).await;
            (second, body)
        });
        let early = overlap(started, &mut task).await;
        first
            .end("COMMIT")
            .await
            .expect("first transaction commits");
        let mut committed = vec![7];
        let (second, body) = match early {
            Some(result) => result,
            None => task.await.unwrap(),
        };
        let outcome = match body {
            Ok(()) => second.end("COMMIT").await,
            Err(error) => Err(error),
        };
        match outcome {
            Ok(()) => committed.push(11),
            Err(error) => {
                eprintln!("explicit stock transaction abort (not committed): {error:?}");
                assert_eq!(
                    error.code().map(|code| code.code()),
                    Some("40001"),
                    "unexpected stock workload failure, not a serialization conflict"
                );
                second.end("ROLLBACK").await.unwrap();
            }
        }
        // A complete stock body followed by deliberate rollback contributes zero.
        sql(&second.client, "BEGIN").await;
        stock_body(&second.client, 101).await.unwrap();
        second.end("ROLLBACK").await.unwrap();
        eprintln!("successful COMMIT deltas: {committed:?}; deliberately rolled back delta: 101");
        conservation(&first.client, &committed).await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_hot_row_update_waits_and_re_evaluates_without_forced_abort() {
    bounded(async {
        let server = pg_server::start_server_with_ddl().await;
        let first = Session::connect(server.port).await;
        let second = Session::connect(server.port).await;
        seed(&first.client).await;
        sql(&first.client, "BEGIN").await;
        sql(&second.client, "BEGIN").await;
        update(&first.client, "UPDATE pgbench_accounts SET abalance = abalance + 7 WHERE aid = 1").await.unwrap();
        let started = second.arm_request();
        let mut task = tokio::spawn(async move {
            let result = update(&second.client,
                "UPDATE pgbench_accounts SET abalance = abalance + 11 WHERE aid = 1").await;
            (second, result)
        });
        let early = overlap(started, &mut task).await;
        let finished_before_release = early.is_some();
        // Never await second UPDATE indefinitely before releasing the first lock.
        first.end("COMMIT").await.unwrap();
        let (second, updated) = match early {
            Some(result) => result,
            None => task.await.unwrap(),
        };
        let outcome = match updated {
            Ok(()) => second.end("COMMIT").await,
            Err(error) => Err(error),
        };
        let actual = rows(&first.client, "SELECT abalance FROM pgbench_accounts WHERE aid = 1").await;
        eprintln!("default hot-row: completed before first commit={finished_before_release}; second outcome={outcome:?}; final={actual:?}");
        assert!(outcome.is_ok(),
            "default PostgreSQL hot-row UPDATE must wait/re-evaluate and commit, not force abort: {outcome:?}");
        assert!(!finished_before_release, "conflicting UPDATE acknowledged before first writer released its row");
        assert_eq!(actual, vec![vec!["18"]], "RHS must use the committed row after waiting");
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_read_committed_accepts_and_refreshes_each_statement() {
    bounded(async {
        let server = pg_server::start_server_with_ddl().await;
        let reader = Session::connect(server.port).await;
        let writer = Session::connect(server.port).await;
        seed(&writer.client).await;
        reader.client.simple_query("BEGIN ISOLATION LEVEL READ COMMITTED").await
            .expect("explicit READ COMMITTED must be accepted for PostgreSQL default-isolation compatibility");
        let select = "SELECT abalance FROM pgbench_accounts WHERE aid = 1";
        assert_eq!(rows(&reader.client, select).await, vec![vec!["0"]]);
        sql(&writer.client, "BEGIN").await;
        update(&writer.client, "UPDATE pgbench_accounts SET abalance = abalance + 7 WHERE aid = 1").await.unwrap();
        assert_eq!(rows(&reader.client, select).await, vec![vec!["0"]], "no dirty reads");
        writer.end("COMMIT").await.unwrap();
        assert_eq!(rows(&reader.client, select).await, vec![vec!["7"]],
            "next READ COMMITTED statement must see the newly committed row");
        reader.end("COMMIT").await.unwrap();
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_rechecks_predicate_after_winner_commit() {
    bounded(async {
        let server = pg_server::start_server_with_ddl().await;
        let first = Session::connect(server.port).await;
        let second = Session::connect(server.port).await;
        seed(&first.client).await;
        sql(&first.client, "BEGIN").await;
        update(&first.client, "UPDATE pgbench_accounts SET abalance = 7 WHERE aid = 1").await.unwrap();
        sql(&second.client, "BEGIN").await;
        let started = second.arm_request();
        let mut task = tokio::spawn(async move {
            let result = second.client.simple_query("UPDATE pgbench_accounts SET abalance = abalance + 11 WHERE aid = 1 AND abalance = 0").await;
            (second, result)
        });
        let early = overlap(started, &mut task).await;
        first.end("COMMIT").await.unwrap();
        let (second, result) = match early { Some(r) => r, None => task.await.unwrap() };
        let messages = result.unwrap();
        assert!(messages.iter().any(|m| matches!(m, SimpleQueryMessage::CommandComplete(0))), "changed predicate must skip the row: {messages:?}");
        second.end("COMMIT").await.unwrap();
        assert_eq!(rows(&first.client, "SELECT abalance FROM pgbench_accounts").await, vec![vec!["7"]]);
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autocommit_waits_for_explicit_writer_and_preserves_disjoint_progress() {
    bounded(async {
        let server = pg_server::start_server_with_ddl().await;
        let first = Session::connect(server.port).await;
        let second = Session::connect(server.port).await;
        let disjoint = Session::connect(server.port).await;
        seed(&first.client).await;
        sql(
            &first.client,
            "INSERT INTO pgbench_accounts (aid, bid, abalance) VALUES (2, 1, 0)",
        )
        .await;
        sql(&first.client, "BEGIN").await;
        update(
            &first.client,
            "UPDATE pgbench_accounts SET abalance = abalance + 7 WHERE aid = 1",
        )
        .await
        .unwrap();
        // Must finish while the first writer still owns a different key.
        tokio::time::timeout(
            Duration::from_secs(2),
            update(
                &disjoint.client,
                "UPDATE pgbench_accounts SET abalance = abalance + 3 WHERE aid = 2",
            ),
        )
        .await
        .unwrap()
        .unwrap();
        let started = second.arm_request();
        let mut task = tokio::spawn(async move {
            let result = update(
                &second.client,
                "UPDATE pgbench_accounts SET abalance = abalance + 11 WHERE aid = 1",
            )
            .await;
            (second, result)
        });
        let early = overlap(started, &mut task).await;
        let premature = early.is_some();
        first.end("COMMIT").await.unwrap();
        let (_, result) = match early {
            Some(r) => r,
            None => task.await.unwrap(),
        };
        result.unwrap();
        assert!(!premature, "autocommit bypassed the open writer");
        assert_eq!(
            rows(
                &first.client,
                "SELECT abalance FROM pgbench_accounts WHERE aid = 1"
            )
            .await,
            vec![vec!["18"]]
        );
        assert_eq!(
            rows(
                &first.client,
                "SELECT abalance FROM pgbench_accounts WHERE aid = 2"
            )
            .await,
            vec![vec!["3"]]
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_ownership_releases_on_rollback_error_and_disconnect() {
    bounded(async {
        let server = pg_server::start_server_with_ddl().await;
        let observer = Session::connect(server.port).await;
        seed(&observer.client).await;
        for ending in ["ROLLBACK", "ERROR", "DISCONNECT"] {
            let first = Session::connect(server.port).await;
            let second = Session::connect(server.port).await;
            sql(&first.client, "BEGIN").await;
            update(
                &first.client,
                "UPDATE pgbench_accounts SET abalance = 100 WHERE aid = 1",
            )
            .await
            .unwrap();
            sql(&second.client, "BEGIN").await;
            let started = second.arm_request();
            let mut task = tokio::spawn(async move {
                let result = update(
                    &second.client,
                    "UPDATE pgbench_accounts SET abalance = abalance + 1 WHERE aid = 1",
                )
                .await;
                (second, result)
            });
            let early = overlap(started, &mut task).await;
            let premature = early.is_some();
            match ending {
                "ROLLBACK" => first.end("ROLLBACK").await.unwrap(),
                "ERROR" => {
                    first
                        .client
                        .simple_query("SELECT missing FROM pgbench_accounts")
                        .await
                        .unwrap_err();
                    let error = first
                        .client
                        .simple_query("UPDATE pgbench_accounts SET abalance = 99 WHERE aid = 1")
                        .await
                        .unwrap_err();
                    assert_eq!(error.code().map(|c| c.code()), Some("25P02"));
                    // Keep the failed session alive: an error must release ownership now.
                }
                "DISCONNECT" => first.driver.abort(),
                _ => unreachable!(),
            }
            let (second, result) = match early {
                Some(r) => r,
                None => task.await.unwrap(),
            };
            result.unwrap();
            second.end("COMMIT").await.unwrap();
            assert!(!premature, "writer was acknowledged before {ending}");
            if ending == "ERROR" {
                first.client.simple_query("COMMIT").await.unwrap();
                assert_eq!(first.wire.lock().unwrap().tags.last().unwrap(), "ROLLBACK");
            }
        }
        assert_eq!(
            rows(&observer.client, "SELECT abalance FROM pgbench_accounts").await,
            vec![vec!["3"]]
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opposite_key_order_has_bounded_failure_and_atomic_rollback() {
    bounded(async {
        let server = pg_server::start_server_with_ddl().await;
        let first = Session::connect(server.port).await;
        let second = Session::connect(server.port).await;
        seed(&first.client).await;
        sql(
            &first.client,
            "INSERT INTO pgbench_accounts (aid, bid, abalance) VALUES (2, 1, 0)",
        )
        .await;
        sql(&first.client, "BEGIN").await;
        sql(&second.client, "BEGIN").await;
        update(
            &first.client,
            "UPDATE pgbench_accounts SET abalance = 7 WHERE aid = 1",
        )
        .await
        .unwrap();
        update(
            &second.client,
            "UPDATE pgbench_accounts SET abalance = 11 WHERE aid = 2",
        )
        .await
        .unwrap();
        let (a, b) = tokio::join!(
            first
                .client
                .simple_query("UPDATE pgbench_accounts SET abalance = 7 WHERE aid = 2"),
            second
                .client
                .simple_query("UPDATE pgbench_accounts SET abalance = 11 WHERE aid = 1")
        );
        assert!(
            a.is_err() || b.is_err(),
            "cross-key ownership must time out or detect deadlock"
        );
        for error in [a.err(), b.err()].into_iter().flatten() {
            assert!(
                matches!(error.code().map(|c| c.code()), Some("55P03" | "40P01")),
                "retain real lock failure code: {error:?}"
            );
        }
        first.end("ROLLBACK").await.unwrap();
        second.end("ROLLBACK").await.unwrap();
        assert_eq!(
            rows(&first.client, "SELECT abalance FROM pgbench_accounts").await,
            vec![vec!["0"], vec!["0"]]
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_rechecks_predicate_after_wait_and_does_not_delete_winner() {
    bounded(async {
        let server = pg_server::start_server_with_ddl().await;
        let first = Session::connect(server.port).await;
        let second = Session::connect(server.port).await;
        seed(&first.client).await;
        sql(&first.client, "BEGIN").await;
        update(
            &first.client,
            "UPDATE pgbench_accounts SET abalance = 7 WHERE aid = 1",
        )
        .await
        .unwrap();
        let started = second.arm_request();
        let mut task = tokio::spawn(async move {
            second
                .client
                .simple_query("DELETE FROM pgbench_accounts WHERE aid = 1 AND abalance = 0")
                .await
        });
        let early = overlap(started, &mut task).await;
        first.end("COMMIT").await.unwrap();
        let messages = match early {
            Some(r) => r,
            None => task.await.unwrap(),
        }
        .unwrap();
        assert!(
            messages
                .iter()
                .any(|m| matches!(m, SimpleQueryMessage::CommandComplete(0))),
            "DELETE must recheck its predicate: {messages:?}"
        );
        assert_eq!(
            rows(&first.client, "SELECT abalance FROM pgbench_accounts").await,
            vec![vec!["7"]]
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_explicit_transaction_stays_aborted_across_extended_sync() {
    bounded(async {
        let server = pg_server::start_server_with_ddl().await;
        let session = Session::connect(server.port).await;
        seed(&session.client).await;
        let stmt = session
            .client
            .prepare("UPDATE pgbench_accounts SET abalance = abalance + $1 WHERE aid = $2")
            .await
            .unwrap();
        sql(&session.client, "BEGIN").await;
        let invalid = session
            .client
            .simple_query("SELECT missing FROM pgbench_accounts")
            .await
            .unwrap_err();
        assert_eq!(invalid.code().map(|c| c.code()), Some("42703"));
        for _ in 0..2 {
            let error = session
                .client
                .execute(&stmt, &[&1_i32, &1_i32])
                .await
                .unwrap_err();
            assert_eq!(error.code().map(|c| c.code()), Some("25P02"));
        }
        session.client.simple_query("COMMIT").await.unwrap();
        assert_eq!(
            session.wire.lock().unwrap().tags.last().unwrap(),
            "ROLLBACK"
        );
        assert_eq!(
            rows(&session.client, "SELECT abalance FROM pgbench_accounts").await,
            vec![vec!["0"]]
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_key_hot_row_rechecks_winner_and_own_writes() {
    bounded(async {
        let server = pg_server::start_server_with_ddl().await;
        let first = Session::connect(server.port).await;
        let second = Session::connect(server.port).await;
        server.engine.register_system_tables().unwrap();
        sql(
            &first.client,
            "CREATE TABLE indexed_stock (aid integer, abalance integer)",
        )
        .await;
        sql(
            &first.client,
            "INSERT INTO indexed_stock (aid, abalance) VALUES (1, 0)",
        )
        .await;
        sql(
            &first.client,
            "ALTER TABLE indexed_stock ADD PRIMARY KEY (aid)",
        )
        .await;
        sql(&first.client, "BEGIN").await;
        update(
            &first.client,
            "UPDATE indexed_stock SET abalance = abalance + 7 WHERE aid = 1",
        )
        .await
        .unwrap();
        update(
            &first.client,
            "UPDATE indexed_stock SET abalance = abalance + 2 WHERE aid = 1",
        )
        .await
        .unwrap();
        sql(&second.client, "BEGIN ISOLATION LEVEL READ COMMITTED").await;
        let started = second.arm_request();
        let mut task = tokio::spawn(async move {
            let result = update(
                &second.client,
                "UPDATE indexed_stock SET abalance = abalance + 11 WHERE aid = 1",
            )
            .await;
            (second, result)
        });
        let early = overlap(started, &mut task).await;
        first.end("COMMIT").await.unwrap();
        let (second, result) = match early {
            Some(r) => r,
            None => task.await.unwrap(),
        };
        result.unwrap();
        assert_eq!(
            rows(
                &second.client,
                "SELECT abalance FROM indexed_stock WHERE aid = 1"
            )
            .await,
            vec![vec!["20"]]
        );
        second.end("COMMIT").await.unwrap();
        assert_eq!(
            rows(
                &first.client,
                "SELECT abalance FROM indexed_stock WHERE aid = 1"
            )
            .await,
            vec![vec!["20"]]
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_does_not_enroll_initially_nonmatching_row_after_wait() {
    bounded(async {
        let server = pg_server::start_server_with_ddl().await;
        let first = Session::connect(server.port).await;
        let second = Session::connect(server.port).await;
        seed(&first.client).await;
        sql(&first.client, "BEGIN").await;
        update(&first.client, "UPDATE pgbench_accounts SET abalance = 7 WHERE aid = 1").await.unwrap();
        let started = second.arm_request();
        let mut task = tokio::spawn(async move {
            second.client.simple_query("UPDATE pgbench_accounts SET abalance = abalance + 11 WHERE aid = 1 AND abalance = 7").await
        });
        let early = overlap(started, &mut task).await;
        first.end("COMMIT").await.unwrap();
        let messages = match early { Some(r) => r, None => task.await.unwrap() }.unwrap();
        assert!(messages.iter().any(|m| matches!(m, SimpleQueryMessage::CommandComplete(0))), "statement must select candidates from its initial committed view: {messages:?}");
        assert_eq!(rows(&first.client, "SELECT abalance FROM pgbench_accounts").await, vec![vec!["7"]]);
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn predicate_update_selects_own_buffered_insert() {
    bounded(async {
        let server = pg_server::start_server_with_ddl().await;
        let session = Session::connect(server.port).await;
        sql(
            &session.client,
            "CREATE TABLE pending_stock (aid integer, abalance integer)",
        )
        .await;
        sql(&session.client, "BEGIN").await;
        sql(
            &session.client,
            "INSERT INTO pending_stock (aid, abalance) VALUES (1, 7)",
        )
        .await;
        update(
            &session.client,
            "UPDATE pending_stock SET abalance = abalance + 11 WHERE aid = 1",
        )
        .await
        .unwrap();
        session.end("COMMIT").await.unwrap();
        assert_eq!(
            rows(&session.client, "SELECT abalance FROM pending_stock").await,
            vec![vec!["18"]]
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_parse_error_aborts_explicit_writer_and_releases_ownership() {
    bounded(async {
        let server = pg_server::start_server_with_ddl().await;
        let first = Session::connect(server.port).await;
        let second = Session::connect(server.port).await;
        seed(&first.client).await;
        sql(&first.client, "BEGIN").await;
        update(
            &first.client,
            "UPDATE pgbench_accounts SET abalance = 7 WHERE aid = 1",
        )
        .await
        .unwrap();
        let error = first.client.prepare("SELECT FROM").await.unwrap_err();
        assert_eq!(error.code().map(|c| c.code()), Some("42601"));
        first.client.simple_query("COMMIT").await.unwrap();
        assert_eq!(
            first.wire.lock().unwrap().tags.last().unwrap(),
            "ROLLBACK",
            "parse errors must abort the explicit transaction"
        );
        update(
            &second.client,
            "UPDATE pgbench_accounts SET abalance = abalance + 11 WHERE aid = 1",
        )
        .await
        .unwrap();
        assert_eq!(
            rows(&second.client, "SELECT abalance FROM pgbench_accounts").await,
            vec![vec!["11"]]
        );
    })
    .await;
}
