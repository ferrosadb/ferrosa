//! t_87e93bce: default waiting is not deadlock detection.
//! Test list: >5s legitimate owner; opposite-order cycle/40P01/atomic cleanup;
//! cancellation by dropping the server execution future releases all ownership.
//! No production clock, timeout, load, or lifecycle settings are overridden.

#[path = "common/pg_server.rs"]
mod pg_server;

use std::future::{poll_fn, Future};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use ferrosa_postgres::{server, QueryContext};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_postgres::{Client, Config, Error, NoTls, SimpleQueryMessage};

const HOLD: Duration = Duration::from_secs(6);
const HANG_GUARD: Duration = Duration::from_secs(30);
const CYCLE_GUARD: Duration = Duration::from_secs(12);

struct Admission {
    expected: Vec<u8>,
    received: Vec<u8>,
    acknowledged: oneshot::Sender<Instant>,
}

type Probe = Arc<Mutex<Option<Admission>>>;

// Server-side transport probe. The acknowledgment is sent by the enclosing
// execution-future poll *after* delivery of the entire expected Query frame.
// This is an execution gate, not internal queue registration. The original RED
// implementation constructed its fixed deadline before its first suspension;
// the GREEN manager has no lock timeout. The owner still stays open for six
// seconds after acknowledged execution, and exact final balances prove recheck.
// Ownership unit tests poll the real acquire future to observe queue admission.
struct ObservedStream {
    socket: TcpStream,
    probe: Probe,
}

impl AsyncRead for ObservedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.socket).poll_read(cx, buf);
        if let Some(probe) = self.probe.lock().unwrap().as_mut() {
            probe.received.extend_from_slice(&buf.filled()[before..]);
            assert!(
                probe.expected.starts_with(&probe.received),
                "instrumented connection must receive exactly the armed Query"
            );
        }
        result
    }
}

impl AsyncWrite for ObservedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.socket).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

struct Session {
    client: Client,
    probe: Probe,
    server: tokio::task::JoinHandle<()>,
    driver: tokio::task::JoinHandle<()>,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.driver.abort();
        self.server.abort();
    }
}

impl Session {
    async fn connect(context: Arc<QueryContext>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (accepted, _) = listener.accept().await.unwrap();
        let probe: Probe = Arc::default();
        let observed = ObservedStream {
            socket: accepted,
            probe: Arc::clone(&probe),
        };
        let server_probe = Arc::clone(&probe);
        let server = tokio::spawn(async move {
            let tls = server::PgTls::plaintext();
            let execution = server::handle_connection(
                observed,
                Arc::new(pg_server::verifier_store()),
                context,
                &tls,
            );
            tokio::pin!(execution);
            poll_fn(|cx| {
                let result = execution.as_mut().poll(cx);
                let mut probe = server_probe.lock().unwrap();
                if probe.as_ref().is_some_and(|p| p.received == p.expected) {
                    let p = probe.take().unwrap();
                    p.acknowledged.send(Instant::now()).unwrap();
                }
                result
            })
            .await
            .expect("server transport");
        });
        let (client, connection) = Config::new()
            .user("ferrosa_user")
            .password("devpass")
            .dbname("ferrosa")
            .connect_raw(socket, NoTls)
            .await
            .expect("SCRAM authentication");
        let driver = tokio::spawn(async move {
            // EOF is expected when the cancellation test drops server execution.
            let _connection_result = connection.await;
        });
        Self {
            client,
            probe,
            server,
            driver,
        }
    }

    fn arm(&self, sql: &str) -> oneshot::Receiver<Instant> {
        let mut expected = vec![b'Q'];
        expected.extend_from_slice(&((sql.len() + 5) as u32).to_be_bytes());
        expected.extend_from_slice(sql.as_bytes());
        expected.push(0);
        let (acknowledged, receive) = oneshot::channel();
        let mut probe = self.probe.lock().unwrap();
        assert!(probe.is_none(), "only one instrumented request in flight");
        *probe = Some(Admission {
            expected,
            received: Vec::new(),
            acknowledged,
        });
        receive
    }
}

async fn bounded(work: impl Future<Output = ()>) {
    tokio::time::timeout(HANG_GUARD, work)
        .await
        .expect("writer wait test hang guard (not a product timeout)");
}

async fn sql(client: &Client, statement: &str) {
    client
        .simple_query(statement)
        .await
        .unwrap_or_else(|e| panic!("{statement}: {e:?}"));
}

async fn update(client: &Client, statement: &str) -> Result<(), Error> {
    let messages = client.simple_query(statement).await?;
    assert!(
        matches!(
            messages.as_slice(),
            [SimpleQueryMessage::CommandComplete(1)]
        ),
        "exactly one row must change: {messages:?}"
    );
    Ok(())
}

async fn balances(client: &Client) -> Vec<Vec<String>> {
    client
        .simple_query("SELECT id, balance FROM wait_rows ORDER BY id")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(vec![
                row.get(0).unwrap().to_owned(),
                row.get(1).unwrap().to_owned(),
            ]),
            _ => None,
        })
        .collect()
}

async fn seed(client: &Client) {
    // Explicit physical keys make row identity deterministic. The live SQL-DDL
    // _sys_ck_ is also per-row, but is randomly generated and striped: two
    // random UUID keys could collide and prevent constructing a two-lock cycle.
    sql(
        client,
        "CREATE TABLE wait_rows (id int PRIMARY KEY, balance int)",
    )
    .await;
    sql(
        client,
        "INSERT INTO wait_rows (id, balance) VALUES (1, 0), (2, 0)",
    )
    .await;
}

#[tokio::test]
async fn default_waiter_survives_five_seconds_then_commits_without_lost_update() {
    bounded(async {
        let fixture = pg_server::start_server_with_ddl().await;
        let owner = Session::connect(Arc::clone(&fixture.context)).await;
        let waiter = Session::connect(Arc::clone(&fixture.context)).await;
        seed(&owner.client).await;
        sql(&owner.client, "BEGIN").await;
        sql(&waiter.client, "BEGIN").await;
        update(&owner.client, "UPDATE wait_rows SET balance = balance + 7 WHERE id = 1").await.unwrap();
        let statement = "UPDATE wait_rows SET balance = balance + 11 WHERE id = 1";
        let admitted = waiter.arm(statement);
        let work = update(&waiter.client, statement);
        tokio::pin!(work);
        let admission = tokio::select! {
            result = &mut work => panic!("waiter completed before admission: {result:?}"),
            admission = admitted => admission.expect("server consumed waiter request"),
        };
        let release_at = admission + HOLD;
        let early = tokio::select! {
            result = &mut work => Some(result),
            () = tokio::time::sleep_until(release_at) => None,
        };
        // Even on RED keep the owner open for the full deliberate interval.
        tokio::time::sleep_until(release_at).await;
        let held = admission.elapsed();
        assert!(held >= HOLD);
        sql(&owner.client, "COMMIT").await;
        let completed_early = early.is_some();
        let result = match early { Some(result) => result, None => work.await };
        eprintln!("default waiter: held after server admission {held:?}; early={completed_early}; result={result:?}");
        sql(&waiter.client, if result.is_ok() { "COMMIT" } else { "ROLLBACK" }).await;
        let actual = balances(&owner.client).await;
        // Reuse the waiter even after RED, proving ownership cleanup before the
        // compatibility assertion. This is a new transaction, never a retry.
        sql(&waiter.client, "BEGIN").await;
        update(&waiter.client, "UPDATE wait_rows SET balance = balance + 1 WHERE id = 1").await.unwrap();
        sql(&waiter.client, "ROLLBACK").await;
        assert_eq!(balances(&owner.client).await, actual, "cleanup probe must roll back");
        assert!(result.is_ok(), "default waiter must survive >5 seconds and complete after owner release: {result:?}");
        assert!(!completed_early, "waiter must not write through an active owner");
        assert_eq!(actual, vec![vec!["1", "18"], vec!["2", "0"]], "RHS must re-read the committed winner; no lost update");
    }).await;
}

#[tokio::test]
async fn opposite_order_cycle_reports_deadlock_and_cleans_up_atomically() {
    bounded(async {
        let fixture = pg_server::start_server_with_ddl().await;
        let first = Session::connect(Arc::clone(&fixture.context)).await;
        let second = Session::connect(Arc::clone(&fixture.context)).await;
        seed(&first.client).await;
        sql(&first.client, "BEGIN").await;
        sql(&second.client, "BEGIN").await;
        update(&first.client, "UPDATE wait_rows SET balance = balance + 7 WHERE id = 1").await.unwrap();
        // This successful ACK while first owns row 1 proves the fixture's two
        // resources are independent, without assuming anything about stripes.
        update(&second.client, "UPDATE wait_rows SET balance = balance + 11 WHERE id = 2").await.unwrap();
        let a_sql = "UPDATE wait_rows SET balance = balance + 7 WHERE id = 2";
        let b_sql = "UPDATE wait_rows SET balance = balance + 11 WHERE id = 1";
        let admitted = first.arm(a_sql);
        let a = update(&first.client, a_sql);
        tokio::pin!(a);
        tokio::select! {
            result = &mut a => panic!("cross-owner update completed before cycle: {result:?}"),
            admission = admitted => { admission.expect("first cross-owner request entered execution"); },
        }
        let (a, b) = tokio::time::timeout(CYCLE_GUARD, async {
            tokio::join!(&mut a, update(&second.client, b_sql))
        }).await.expect("a true wait cycle must resolve without client cancellation");
        eprintln!("opposite-order outcomes: first={a:?}; second={b:?}");
        // Clean up and audit before checking SQLSTATE so RED leaves evidence of
        // rollback atomicity, survivor state, and reusable transaction leases.
        for (session, result) in [(&first, &a), (&second, &b)] {
            if result.is_err() {
                let failed = session.client.simple_query("SELECT 1").await.unwrap_err();
                assert_eq!(failed.code().map(|c| c.code()), Some("25P02"));
            }
            sql(&session.client, if result.is_ok() { "COMMIT" } else { "ROLLBACK" }).await;
        }
        let sum = if a.is_ok() { 7 } else { 0 } + if b.is_ok() { 11 } else { 0 };
        let actual = balances(&first.client).await;
        assert_eq!(actual, vec![vec!["1".to_owned(), sum.to_string()], vec!["2".to_owned(), sum.to_string()]], "victim writes must not leak");
        for session in [&first, &second] {
            sql(&session.client, "BEGIN").await;
            for id in [1, 2] {
                update(&session.client, &format!("UPDATE wait_rows SET balance = balance + 100 WHERE id = {id}")).await.unwrap();
            }
            sql(&session.client, "ROLLBACK").await;
        }
        assert_eq!(balances(&first.client).await, actual, "cleanup transactions roll back");
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1, "one survivor, one deadlock victim");
        for error in [a.err(), b.err()].into_iter().flatten() {
            assert_eq!(error.code().map(|c| c.code()), Some("40P01"), "a real cycle is deadlock_detected, not lock_timeout: {error:?}");
        }
    }).await;
}

#[tokio::test]
async fn dropping_waiting_server_execution_releases_preowned_rows() {
    bounded(async {
        let fixture = pg_server::start_server_with_ddl().await;
        let owner = Session::connect(Arc::clone(&fixture.context)).await;
        let mut cancelled = Session::connect(Arc::clone(&fixture.context)).await;
        seed(&owner.client).await;
        sql(&owner.client, "BEGIN").await;
        sql(&cancelled.client, "BEGIN").await;
        update(
            &owner.client,
            "UPDATE wait_rows SET balance = balance + 7 WHERE id = 1",
        )
        .await
        .unwrap();
        update(
            &cancelled.client,
            "UPDATE wait_rows SET balance = balance + 11 WHERE id = 2",
        )
        .await
        .unwrap();
        let statement = "UPDATE wait_rows SET balance = balance + 11 WHERE id = 1";
        let admitted = cancelled.arm(statement);
        let work = update(&cancelled.client, statement);
        tokio::pin!(work);
        tokio::select! {
            result = &mut work => panic!("waiter completed before cancellation: {result:?}"),
            admission = admitted => { admission.expect("server entered waiting statement"); },
        }
        // This is execution-future cancellation, not a claim that PostgreSQL's
        // separate CancelRequest or peer EOF is currently implemented promptly.
        cancelled.server.abort();
        let result = (&mut cancelled.server).await.unwrap_err();
        assert!(result.is_cancelled());
        assert!(
            work.await.is_err(),
            "cancelled connection cannot acknowledge a write"
        );
        update(
            &owner.client,
            "UPDATE wait_rows SET balance = balance + 7 WHERE id = 2",
        )
        .await
        .unwrap();
        sql(&owner.client, "COMMIT").await;
        assert_eq!(
            balances(&owner.client).await,
            vec![vec!["1", "7"], vec!["2", "7"]],
            "cancelled transaction must not publish its first row"
        );
    })
    .await;
}
