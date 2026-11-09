//! LISTEN / NOTIFY / pg_notify over the wire (D13), through a real driver that receives
//! `NotificationResponse` messages asynchronously.
//!
//! The contract (PostgreSQL's, and `specs/lean4/Ferrosa/Sql/Notify.lean`): delivered only if the
//! notifying transaction commits, at most once, in commit order; a repeated (channel, payload)
//! inside one transaction is delivered once.

#[path = "common/pg_server.rs"]
mod pg_server;

use std::time::Duration;

use futures::{stream, StreamExt};
use tokio::sync::mpsc;
use tokio_postgres::config::SslMode;
use tokio_postgres::{AsyncMessage, Config, NoTls};

/// A client whose connection task forwards every notification (channel, payload).
async fn listener(
    port: u16,
) -> (
    tokio_postgres::Client,
    mpsc::UnboundedReceiver<(String, String)>,
) {
    let (client, mut connection) = Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("ferrosa_user")
        .password("devpass")
        .dbname("ferrosa")
        .ssl_mode(SslMode::Disable)
        .connect(NoTls)
        .await
        .expect("connect");
    let (tx, rx) = mpsc::unbounded_channel();
    let mut messages = stream::poll_fn(move |cx| connection.poll_message(cx));
    tokio::spawn(async move {
        while let Some(message) = messages.next().await {
            match message {
                Ok(AsyncMessage::Notification(n)) => {
                    let _ = tx.send((n.channel().to_string(), n.payload().to_string()));
                }
                Ok(_) => {}
                Err(error) => {
                    eprintln!("listener connection ended: {error}");
                    return;
                }
            }
        }
    });
    (client, rx)
}

async fn next(rx: &mut mpsc::UnboundedReceiver<(String, String)>) -> (String, String) {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a notification arrives within 5s")
        .expect("listener still open")
}

/// Nothing arrives within a short window.
async fn quiet(rx: &mut mpsc::UnboundedReceiver<(String, String)>) {
    if let Ok(Some(n)) = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await {
        panic!("unexpected notification {n:?}");
    }
}

/// Run statements one simple query at a time (the front end takes one statement per query).
async fn run(client: &tokio_postgres::Client, statements: &[&str]) {
    for sql in statements {
        client
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

fn pair(c: &str, p: &str) -> (String, String) {
    (c.to_string(), p.to_string())
}

#[tokio::test]
async fn an_idle_listener_receives_an_autocommit_notify() {
    let server = pg_server::start_server(0).await;
    let (a, mut rx) = listener(server.port).await;
    a.batch_execute("LISTEN jobs").await.expect("LISTEN");
    let b = pg_server::connect(server.port).await;
    b.batch_execute("NOTIFY jobs, 'hello'")
        .await
        .expect("NOTIFY");
    assert_eq!(next(&mut rx).await, pair("jobs", "hello"));
}

#[tokio::test]
async fn a_rolled_back_transaction_notifies_nobody() {
    let server = pg_server::start_server(0).await;
    let (a, mut rx) = listener(server.port).await;
    a.batch_execute("LISTEN jobs").await.expect("LISTEN");
    let b = pg_server::connect(server.port).await;
    run(&b, &["BEGIN", "NOTIFY jobs, 'lost'", "ROLLBACK"]).await;
    b.batch_execute("NOTIFY jobs, 'after'")
        .await
        .expect("NOTIFY");
    assert_eq!(
        next(&mut rx).await,
        pair("jobs", "after"),
        "the rolled-back one never arrives"
    );
}

#[tokio::test]
async fn a_notify_inside_a_transaction_is_delivered_at_commit_not_before() {
    let server = pg_server::start_server(0).await;
    let (a, mut rx) = listener(server.port).await;
    a.batch_execute("LISTEN jobs").await.expect("LISTEN");
    let b = pg_server::connect(server.port).await;
    run(&b, &["BEGIN", "NOTIFY jobs, 'pending'"]).await;
    quiet(&mut rx).await;
    b.batch_execute("COMMIT").await.expect("commit");
    assert_eq!(next(&mut rx).await, pair("jobs", "pending"));
}

#[tokio::test]
async fn notifications_arrive_in_commit_order() {
    let server = pg_server::start_server(0).await;
    let (a, mut rx) = listener(server.port).await;
    a.batch_execute("LISTEN jobs").await.expect("LISTEN");
    let b = pg_server::connect(server.port).await;
    let c = pg_server::connect(server.port).await;
    run(&b, &["BEGIN", "NOTIFY jobs, 'b'"]).await;
    run(&c, &["BEGIN", "NOTIFY jobs, 'c'"]).await;
    c.batch_execute("COMMIT").await.expect("c commits first");
    b.batch_execute("COMMIT").await.expect("b commits second");
    assert_eq!(next(&mut rx).await, pair("jobs", "c"));
    assert_eq!(next(&mut rx).await, pair("jobs", "b"));
}

#[tokio::test]
async fn a_repeated_notification_in_one_transaction_is_delivered_once() {
    let server = pg_server::start_server(0).await;
    let (a, mut rx) = listener(server.port).await;
    a.batch_execute("LISTEN jobs").await.expect("LISTEN");
    let b = pg_server::connect(server.port).await;
    run(
        &b,
        &[
            "BEGIN",
            "NOTIFY jobs, 'x'",
            "NOTIFY jobs, 'x'",
            "NOTIFY jobs, 'y'",
            "COMMIT",
        ],
    )
    .await;
    assert_eq!(next(&mut rx).await, pair("jobs", "x"));
    assert_eq!(next(&mut rx).await, pair("jobs", "y"));
    quiet(&mut rx).await;
}

#[tokio::test]
async fn unlisten_stops_delivery_and_other_channels_are_not_delivered() {
    let server = pg_server::start_server(0).await;
    let (a, mut rx) = listener(server.port).await;
    run(&a, &["LISTEN jobs", "LISTEN other"]).await;
    let b = pg_server::connect(server.port).await;
    b.batch_execute("NOTIFY elsewhere, 'no'")
        .await
        .expect("NOTIFY");
    a.batch_execute("UNLISTEN jobs").await.expect("UNLISTEN");
    run(&b, &["NOTIFY jobs, 'no'", "NOTIFY other, 'yes'"]).await;
    assert_eq!(next(&mut rx).await, pair("other", "yes"));
    a.batch_execute("UNLISTEN *").await.expect("UNLISTEN *");
    b.batch_execute("NOTIFY other, 'no'").await.expect("NOTIFY");
    quiet(&mut rx).await;
}

#[tokio::test]
async fn pg_notify_works_in_simple_and_extended_protocols() {
    let server = pg_server::start_server(0).await;
    let (a, mut rx) = listener(server.port).await;
    a.batch_execute("LISTEN jobs").await.expect("LISTEN");
    let b = pg_server::connect(server.port).await;
    b.simple_query("SELECT pg_notify('jobs', 'simple')")
        .await
        .expect("simple pg_notify");
    assert_eq!(next(&mut rx).await, pair("jobs", "simple"));
    let rows = b
        .query("SELECT pg_notify($1, $2)", &[&"jobs", &"extended"])
        .await
        .expect("extended pg_notify");
    assert_eq!(rows.len(), 1, "pg_notify returns one (void) row");
    assert_eq!(next(&mut rx).await, pair("jobs", "extended"));
}

#[tokio::test]
async fn a_listener_receives_its_own_committed_notification() {
    let server = pg_server::start_server(0).await;
    let (a, mut rx) = listener(server.port).await;
    run(&a, &["LISTEN jobs", "NOTIFY jobs, 'self'"]).await;
    assert_eq!(next(&mut rx).await, pair("jobs", "self"));
}

#[tokio::test]
async fn listen_inside_a_rolled_back_transaction_does_not_subscribe() {
    let server = pg_server::start_server(0).await;
    let (a, mut rx) = listener(server.port).await;
    run(&a, &["BEGIN", "LISTEN jobs", "ROLLBACK"]).await;
    let b = pg_server::connect(server.port).await;
    b.batch_execute("NOTIFY jobs, 'no'").await.expect("NOTIFY");
    quiet(&mut rx).await;
}

#[tokio::test]
async fn an_over_long_payload_and_an_empty_channel_are_refused() {
    let server = pg_server::start_server(0).await;
    let b = pg_server::connect(server.port).await;
    let long = "x".repeat(8000);
    let error = b
        .batch_execute(&format!("NOTIFY jobs, '{long}'"))
        .await
        .expect_err("payload too long");
    assert_eq!(error.code().map(|c| c.code()), Some("22023"));
    let error = b
        .simple_query("SELECT pg_notify('', 'x')")
        .await
        .expect_err("empty channel");
    assert_eq!(error.code().map(|c| c.code()), Some("22023"));
}

#[tokio::test]
async fn a_cluster_node_refuses_listen_rather_than_miss_other_nodes_notifications() {
    let committer: std::sync::Arc<dyn ferrosa_storage::accord::TransactionCommitter> =
        std::sync::Arc::new(ferrosa_storage::accord::MockTransactionCommitter::new());
    let server = pg_server::start_server_with_accord(
        0,
        ferrosa_postgres::PortalLimits::default(),
        ferrosa_postgres::AccordAccess::fixed(committer),
    )
    .await;
    let a = pg_server::connect(server.port).await;
    let error = a.batch_execute("LISTEN jobs").await.expect_err("refused");
    assert_eq!(error.code().map(|c| c.code()), Some("0A000"));
    // NOTIFY is still accepted: with no listener anywhere, delivering it to nobody is exact.
    a.batch_execute("NOTIFY jobs, 'x'").await.expect("NOTIFY");
}
