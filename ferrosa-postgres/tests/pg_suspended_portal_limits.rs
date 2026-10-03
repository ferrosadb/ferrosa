//! Suspended portals are capped per connection and per node, and the
//! (limit+1)th is refused with SQLSTATE 53000; closing one frees its place.
//!
//! A suspended portal holds no thread, but it holds its query: buffered rows,
//! the storage scan's open SSTable readers, spilled sort runs. The caps are
//! the safety net that bounds how much of that idle clients can pile up.

#[path = "common/pg_server.rs"]
mod pg_server;

use std::time::Duration;

use ferrosa_postgres::PortalLimits;
use pg_server::{connect, start_server_with};
use tokio_postgres::error::SqlState;

const ROWS: usize = 500;

fn limits() -> PortalLimits {
    PortalLimits {
        per_connection: 2,
        per_node: 3,
        idle_timeout: Duration::from_secs(600),
    }
}

/// The SQLSTATE of a failed driver call.
fn sqlstate(error: &tokio_postgres::Error) -> SqlState {
    error
        .code()
        .cloned()
        .unwrap_or_else(|| panic!("expected a server error with a SQLSTATE, got {error}"))
}

/// The server's message text of a failed driver call.
fn message(error: &tokio_postgres::Error) -> String {
    error
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_else(|| panic!("expected a server error, got {error}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_limits_refuse_one_more_suspended_portal_and_closing_one_frees_it() {
    ferrosa_sched::init_global_pool(ferrosa_sched::Reservation::new(17, 1));
    let server = start_server_with(ROWS, limits()).await;

    // Per connection: two suspend, the third is refused with 53000.
    let mut first = connect(server.port).await;
    {
        let tx = first.transaction().await.expect("begin");
        let statement = tx.prepare("SELECT id FROM t").await.expect("prepare");
        let mut portals = Vec::new();
        for _ in 0..2 {
            let portal = tx.bind(&statement, &[]).await.expect("bind");
            assert_eq!(tx.query_portal(&portal, 1).await.expect("execute").len(), 1);
            portals.push(portal);
        }
        assert_eq!(server.portals.suspended(), 2);
        let third = tx.bind(&statement, &[]).await.expect("bind");
        let refusal = tx
            .query_portal(&third, 1)
            .await
            .expect_err("a third suspended portal is over the connection limit");
        assert_eq!(sqlstate(&refusal), SqlState::INSUFFICIENT_RESOURCES);
        let text = message(&refusal);
        assert!(
            text.contains("this connection"),
            "the message names the limit: {text}"
        );
        assert_eq!(
            server.portals.suspended(),
            2,
            "the refused portal holds nothing"
        );
        // The error aborted the transaction; ending it releases its portals.
        drop(portals);
        tx.rollback().await.expect("rollback");
    }
    assert_eq!(
        server.portals.suspended(),
        0,
        "ending the transaction freed every slot"
    );

    // Closing one frees a place.
    let tx = first.transaction().await.expect("begin");
    let statement = tx.prepare("SELECT id FROM t").await.expect("prepare");
    let a = tx.bind(&statement, &[]).await.expect("bind");
    let b = tx.bind(&statement, &[]).await.expect("bind");
    tx.query_portal(&a, 1).await.expect("a suspends");
    tx.query_portal(&b, 1).await.expect("b suspends");
    drop(a); // the driver sends Close with its next message
    let c = tx.bind(&statement, &[]).await.expect("bind");
    let rows = tx
        .query_portal(&c, 1)
        .await
        .expect("closing a portal frees its place on the connection");
    assert_eq!(rows.len(), 1);
    assert_eq!(server.portals.suspended(), 2);

    // Per node: this connection holds 2 of 3, so another connection gets one
    // and is refused the next with the node's message.
    let mut second = connect(server.port).await;
    let other = second.transaction().await.expect("begin");
    let statement = other.prepare("SELECT id FROM t").await.expect("prepare");
    let d = other.bind(&statement, &[]).await.expect("bind");
    other
        .query_portal(&d, 1)
        .await
        .expect("the node has room for one");
    assert_eq!(server.portals.suspended(), 3);
    let e = other.bind(&statement, &[]).await.expect("bind");
    let refusal = other
        .query_portal(&e, 1)
        .await
        .expect_err("the node is at its limit");
    assert_eq!(sqlstate(&refusal), SqlState::INSUFFICIENT_RESOURCES);
    let text = message(&refusal);
    assert!(
        text.contains("this node"),
        "the message names the limit: {text}"
    );
    drop((b, c, d, e));
    other.rollback().await.expect("rollback");
    tx.rollback().await.expect("rollback");
    assert_eq!(server.portals.suspended(), 0);
}

/// A portal run to its end answers a further `Execute` with no rows, as
/// PostgreSQL does; it never re-runs the query.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_finished_portal_returns_no_more_rows() {
    ferrosa_sched::init_global_pool(ferrosa_sched::Reservation::new(17, 1));
    let server = start_server_with(ROWS, limits()).await;
    let mut client = connect(server.port).await;
    let tx = client.transaction().await.expect("begin");
    let statement = tx.prepare("SELECT id FROM t").await.expect("prepare");
    let portal = tx.bind(&statement, &[]).await.expect("bind");
    let mut total = 0;
    loop {
        let rows = tx.query_portal(&portal, 200).await.expect("execute");
        total += rows.len();
        if rows.len() < 200 {
            break;
        }
    }
    assert_eq!(total, ROWS);
    let again = tx.query_portal(&portal, 200).await.expect("execute");
    assert!(
        again.is_empty(),
        "a finished portal re-ran its query and returned {} rows again",
        again.len()
    );
    tx.commit().await.expect("commit");
}
