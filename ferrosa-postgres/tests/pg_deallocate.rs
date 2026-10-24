//! `DEALLOCATE` over the PostgreSQL wire protocol (issue #609).
//!
//! Drivers prepare statements server-side and then deallocate them by name.
//! psycopg 3, on a libpq older than 17, closes each statement it prepared with
//! `DEALLOCATE <name>` (its names are `_pg3_0`, `_pg3_1`, …), and clears the
//! whole cache with `DEALLOCATE ALL` (`psycopg/_connection_base.py`,
//! `_deallocate`). Before this fix every one of those statements was rejected
//! with a syntax error, so a client could only close a prepared connection
//! cleanly by turning the feature off (`prepare_threshold=None`).
//!
//! These tests drive the protocol frames directly: `tokio-postgres` assigns
//! statement names itself (`s0`, `s1`, …) and exposes no way to choose one, and
//! every invariant here is about a named statement. PostgreSQL behaviour was
//! verified against a real PostgreSQL 18 server (see the assertions on command
//! tags and identifiers below).
//!
//! Every test asserts the *protocol* reply: the command tag, or the SQLSTATE.

#[path = "common/pg_server.rs"]
mod pg_server;
#[path = "common/pg_wire.rs"]
mod pg_wire;

use pg_server::start_server;
use pg_wire::{assert_sqlstate, command_tag, Frame, WireClient};

/// A prepared statement that runs over the seeded `ks.t` (three partitions).
const SELECT: &str = "SELECT id FROM t";

const ROWS: usize = 3;

async fn wire(port: u16) -> WireClient {
    WireClient::connect(port, "ferrosa_user", "devpass").await
}

/// Assert the frames carry no `ErrorResponse` and one `CommandComplete` whose
/// tag starts with `prefix`; return the tag.
fn assert_completed(frames: &[Frame], prefix: &str) -> String {
    let tag = command_tag(frames);
    assert!(
        tag.starts_with(prefix),
        "expected a `{prefix}` completion, got `{tag}`"
    );
    tag
}

/// I1: `DEALLOCATE ALL` succeeds and clears the session's registry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deallocate_all_succeeds_and_clears_the_session_registry() {
    let server = start_server(ROWS).await;
    let mut client = wire(server.port).await;

    client.parse("prep_a", SELECT).await;
    client.parse("prep_b", SELECT).await;
    // Both statements are live before the deallocation.
    assert_completed(&client.bind_execute("", "prep_a").await, "SELECT");
    assert_completed(&client.bind_execute("", "prep_b").await, "SELECT");

    let frames = client.simple("DEALLOCATE ALL").await;
    assert_eq!(
        command_tag(&frames),
        "DEALLOCATE ALL",
        "PostgreSQL answers `DEALLOCATE ALL` with that exact tag"
    );

    // The registry is empty: neither name can be bound any more.
    assert_sqlstate(&client.bind_execute("", "prep_a").await, "26000");
    assert_sqlstate(&client.bind_execute("", "prep_b").await, "26000");

    client.terminate().await;
}

/// I2: `DEALLOCATE prep_1` succeeds, that name is gone, and executing it fails
/// the way PostgreSQL fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deallocate_named_statement_removes_it_and_execute_then_fails() {
    let server = start_server(ROWS).await;
    let mut client = wire(server.port).await;

    client.parse("prep_1", SELECT).await;
    assert_completed(&client.bind_execute("", "prep_1").await, "SELECT");

    let frames = client.simple("DEALLOCATE prep_1").await;
    assert_eq!(
        command_tag(&frames),
        "DEALLOCATE",
        "deallocating one statement answers the `DEALLOCATE` tag"
    );

    // PostgreSQL: `prepared statement "prep_1" does not exist`, SQLSTATE 26000.
    let message = assert_sqlstate(&client.bind_execute("", "prep_1").await, "26000");
    assert_eq!(
        message, "prepared statement \"prep_1\" does not exist",
        "the message must match PostgreSQL's"
    );

    client.terminate().await;
}

/// I3: deallocating a name that does not exist is `invalid_sql_statement_name`
/// (26000), NOT a syntax error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deallocate_of_a_missing_name_is_invalid_sql_statement_name() {
    let server = start_server(ROWS).await;
    let mut client = wire(server.port).await;

    let frames = client.simple("DEALLOCATE prep_missing").await;
    let message = assert_sqlstate(&frames, "26000");
    assert!(
        message.contains("prep_missing"),
        "the error must name the statement: {message}"
    );
    assert_eq!(
        message, "prepared statement \"prep_missing\" does not exist",
        "the message must match PostgreSQL's"
    );
    assert!(
        !frames
            .iter()
            .any(|frame| frame.sqlstate().as_deref() == Some("42601")),
        "a missing name is 26000, never a syntax error"
    );

    client.terminate().await;
}

/// I4: a connection that prepared six statements (psycopg's sequence) closes
/// cleanly after deallocating them — no server error on any step, and a clean
/// close after `Terminate`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_that_prepared_six_statements_closes_cleanly_after_deallocate() {
    let server = start_server(ROWS).await;
    let mut client = wire(server.port).await;

    // psycopg 3 names what it prepares `_pg3_<n>` and executes each once;
    // `prepare_threshold = 1` reaches six prepares after six distinct commands.
    for n in 0..6 {
        let frames = client.parse(&format!("_pg3_{n}"), SELECT).await;
        assert_eq!(
            frames[0].tag,
            b'1',
            "Parse must answer ParseComplete, got {:?}",
            frames[0].describe()
        );
        assert_completed(
            &client
                .bind_execute(&format!("p{n}"), &format!("_pg3_{n}"))
                .await,
            "SELECT",
        );
    }

    // Closing the connection: each prepared statement is deallocated by name,
    // then the cache clear runs `DEALLOCATE ALL`.
    for n in 0..6 {
        let frames = client.simple(&format!("DEALLOCATE _pg3_{n}")).await;
        assert_eq!(command_tag(&frames), "DEALLOCATE");
    }
    let frames = client.simple("DEALLOCATE ALL").await;
    assert_eq!(command_tag(&frames), "DEALLOCATE ALL");

    // Terminate closes the socket without a word from the server.
    let trailing = client.terminate().await;
    assert!(
        trailing.is_empty(),
        "the server must not answer an error to a clean close: {:?}",
        trailing.iter().map(Frame::describe).collect::<Vec<_>>()
    );
}

/// I5: deallocating one statement leaves every other prepared statement usable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deallocating_one_statement_leaves_the_others_usable() {
    let server = start_server(ROWS).await;
    let mut client = wire(server.port).await;

    client.parse("stmt_keep", SELECT).await;
    client.parse("stmt_drop", SELECT).await;

    let frames = client.simple("DEALLOCATE stmt_drop").await;
    assert_eq!(command_tag(&frames), "DEALLOCATE");

    // The untouched statement still runs, and still returns rows.
    let frames = client.bind_execute("", "stmt_keep").await;
    let tag = assert_completed(&frames, "SELECT");
    assert_eq!(
        tag,
        format!("SELECT {ROWS}"),
        "the surviving statement must still return its rows"
    );
    assert!(
        frames.iter().filter(|frame| frame.tag == b'D').count() == ROWS,
        "expected {ROWS} DataRows"
    );

    // …while the deallocated one is gone.
    assert_sqlstate(&client.bind_execute("", "stmt_drop").await, "26000");

    client.terminate().await;
}

/// PostgreSQL allows `DEALLOCATE` inside a transaction block, and a prepared
/// statement is session state: `ROLLBACK` does not bring one back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deallocate_is_allowed_inside_a_transaction_block_and_rollback_does_not_undo_it() {
    let server = start_server(ROWS).await;
    let mut client = wire(server.port).await;

    client.parse("tx_stmt", SELECT).await;
    assert_eq!(command_tag(&client.simple("BEGIN").await), "BEGIN");

    let frames = client.simple("DEALLOCATE tx_stmt").await;
    assert_eq!(
        command_tag(&frames),
        "DEALLOCATE",
        "DEALLOCATE is a utility command: PostgreSQL accepts it in a transaction block"
    );

    assert_eq!(command_tag(&client.simple("ROLLBACK").await), "ROLLBACK");
    assert_sqlstate(&client.bind_execute("", "tx_stmt").await, "26000");

    client.terminate().await;
}

/// The identifier rules PostgreSQL applies to the name: an unquoted name folds
/// to lower case, a quoted one is exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unquoted_names_fold_to_lowercase_and_quoted_names_are_exact() {
    let server = start_server(ROWS).await;
    let mut client = wire(server.port).await;

    // A statement name from the extended-protocol Parse is used literally.
    client.parse("Mixed_Case", SELECT).await;

    // `DEALLOCATE Mixed_Case` names `mixed_case`, which does not exist.
    let frames = client.simple("DEALLOCATE Mixed_Case").await;
    assert_sqlstate(&frames, "26000");

    // Quoting addresses the exact name.
    let frames = client.simple("DEALLOCATE \"Mixed_Case\"").await;
    assert_eq!(command_tag(&frames), "DEALLOCATE");
    assert_sqlstate(&client.bind_execute("", "Mixed_Case").await, "26000");

    client.terminate().await;
}

/// `DEALLOCATE PREPARE <name>` is the same statement; a quoted `"all"` is a
/// name, not the `ALL` keyword; and junk after the target is still a syntax
/// error (42601).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deallocate_accepts_the_prepare_keyword_and_a_quoted_all_is_a_name() {
    let server = start_server(ROWS).await;
    let mut client = wire(server.port).await;

    client.parse("all", SELECT).await;
    client.parse("other", SELECT).await;

    // Quoted `"all"` is the statement called `all`; `other` is untouched.
    let frames = client.simple("DEALLOCATE \"all\"").await;
    assert_eq!(command_tag(&frames), "DEALLOCATE");
    assert_sqlstate(&client.bind_execute("", "all").await, "26000");
    assert_completed(&client.bind_execute("", "other").await, "SELECT");

    // The `PREPARE` keyword form.
    let frames = client.simple("DEALLOCATE PREPARE other").await;
    assert_eq!(command_tag(&frames), "DEALLOCATE");
    assert_sqlstate(&client.bind_execute("", "other").await, "26000");

    // Anything after the target is a syntax error, as in PostgreSQL.
    let frames = client.simple("DEALLOCATE ALL EXTRA").await;
    assert_sqlstate(&frames, "42601");

    client.terminate().await;
}
