//! `DO $$ … $$` over the wire (D13): interpreted against the calling session, inside its
//! transaction, with every statement routed through the normal front-end dispatch.

#[path = "common/pg_server.rs"]
mod pg_server;

async fn count(client: &tokio_postgres::Client, id: &str) -> usize {
    client
        .query("SELECT name FROM ks.t WHERE id = $1", &[&id])
        .await
        .expect("count query")
        .len()
}

fn code(error: &tokio_postgres::Error) -> String {
    error
        .code()
        .map(|c| c.code().to_string())
        .unwrap_or_else(|| format!("no SQLSTATE: {error}"))
}

fn message(error: &tokio_postgres::Error) -> String {
    error
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_else(|| error.to_string())
}

const INSERT_ONCE: &str = "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM ks.t WHERE id = 'k1') THEN INSERT INTO ks.t (id, ck, name, score) VALUES ('k1', 1, 'first', 1); END IF; END $$";

#[tokio::test]
async fn the_census_shape_runs_its_body_once_and_is_idempotent() {
    let server = pg_server::start_server(0).await;
    let client = pg_server::connect(server.port).await;
    client.batch_execute(INSERT_ONCE).await.expect("DO runs");
    assert_eq!(count(&client, "k1").await, 1);
    client
        .batch_execute(INSERT_ONCE)
        .await
        .expect("DO runs again");
    assert_eq!(
        count(&client, "k1").await,
        1,
        "the EXISTS guard saw the row"
    );
}

#[tokio::test]
async fn a_do_block_inside_a_rolled_back_transaction_leaves_nothing() {
    let server = pg_server::start_server(0).await;
    let client = pg_server::connect(server.port).await;
    client.batch_execute("BEGIN").await.expect("BEGIN");
    client.batch_execute(INSERT_ONCE).await.expect("DO runs");
    assert_eq!(
        count(&client, "k1").await,
        1,
        "visible inside its own transaction"
    );
    client.batch_execute("ROLLBACK").await.expect("ROLLBACK");
    assert_eq!(
        count(&client, "k1").await,
        0,
        "rolled back with the transaction"
    );
}

#[tokio::test]
async fn a_failing_do_block_in_autocommit_applies_nothing() {
    let server = pg_server::start_server(0).await;
    let client = pg_server::connect(server.port).await;
    let error = client
        .batch_execute(
            "DO $$ BEGIN INSERT INTO ks.t (id, ck, name, score) VALUES ('k2', 1, 'x', 1); INSERT INTO ks.missing (id) VALUES ('y'); END $$",
        )
        .await
        .expect_err("the second insert fails");
    assert_eq!(code(&error), "42P01");
    assert_eq!(
        count(&client, "k2").await,
        0,
        "the first insert went with it"
    );
}

#[tokio::test]
async fn a_failing_do_block_poisons_an_explicit_transaction() {
    let server = pg_server::start_server(0).await;
    let client = pg_server::connect(server.port).await;
    client.batch_execute("BEGIN").await.expect("BEGIN");
    client
        .batch_execute("DO $$ BEGIN INSERT INTO ks.missing (id) VALUES ('y'); END $$")
        .await
        .expect_err("fails");
    let error = client
        .batch_execute("SELECT 1")
        .await
        .expect_err("poisoned");
    assert_eq!(code(&error), "25P02");
    client.batch_execute("ROLLBACK").await.expect("ROLLBACK");
}

#[tokio::test]
async fn a_do_block_notifies_at_commit() {
    let server = pg_server::start_server(0).await;
    let client = pg_server::connect(server.port).await;
    // The notification is published only by the commit of the DO's own transaction; the
    // NOTIFY tests cover delivery. Here: the DO succeeds and queues through the host.
    client
        .batch_execute("DO $$ BEGIN PERFORM pg_notify('jobs', 'from do'); END $$")
        .await
        .expect("DO with pg_notify");
}

#[tokio::test]
async fn select_into_reads_a_row_into_a_variable_and_binds_it() {
    let server = pg_server::start_server(0).await;
    let client = pg_server::connect(server.port).await;
    client
        .batch_execute("INSERT INTO ks.t (id, ck, name, score) VALUES ('src', 1, 'copied', 1)")
        .await
        .expect("seed");
    client
        .batch_execute(
            "DO $$ DECLARE n text; BEGIN SELECT name INTO n FROM ks.t WHERE id = 'src'; INSERT INTO ks.t (id, ck, name, score) VALUES ('dst', 1, n, 2); END $$",
        )
        .await
        .expect("DO copies through a variable");
    let rows = client
        .query("SELECT name FROM ks.t WHERE id = 'dst'", &[])
        .await
        .expect("read");
    assert_eq!(rows[0].get::<_, String>(0), "copied");
}

#[tokio::test]
async fn a_do_block_runs_over_the_extended_protocol() {
    let server = pg_server::start_server(0).await;
    let client = pg_server::connect(server.port).await;
    client.execute(INSERT_ONCE, &[]).await.expect("extended DO");
    assert_eq!(count(&client, "k1").await, 1);
}

#[tokio::test]
async fn an_unsupported_construct_is_refused_and_runs_nothing() {
    let server = pg_server::start_server(0).await;
    let client = pg_server::connect(server.port).await;
    let error = client
        .batch_execute("DO $$ BEGIN INSERT INTO ks.t (id, ck, name, score) VALUES ('k3', 1, 'x', 1); RAISE NOTICE 'hi'; END $$")
        .await
        .expect_err("RAISE is refused");
    assert_eq!(code(&error), "0A000");
    assert!(message(&error).contains("RAISE"), "{}", message(&error));
    assert_eq!(
        count(&client, "k3").await,
        0,
        "refused at parse, before anything ran"
    );
}

#[tokio::test]
async fn a_do_block_creates_a_table_through_the_ddl_path() {
    let server = pg_server::start_server_with_ddl().await;
    let client = pg_server::connect(server.port).await;
    client
        .batch_execute("DO $$ BEGIN CREATE TABLE made_by_do (id text PRIMARY KEY); END $$")
        .await
        .expect("DDL inside the DO block's transaction");
    client
        .query("SELECT id FROM made_by_do", &[])
        .await
        .expect("the table exists after the DO block commits");
}

#[tokio::test]
async fn a_do_block_that_fails_after_its_ddl_leaves_no_table() {
    let server = pg_server::start_server_with_ddl().await;
    let client = pg_server::connect(server.port).await;
    client
        .batch_execute(
            "DO $$ BEGIN CREATE TABLE half_made (id text PRIMARY KEY); INSERT INTO ks.missing (id) VALUES ('x'); END $$",
        )
        .await
        .expect_err("the insert fails");
    let error = client
        .query("SELECT id FROM half_made", &[])
        .await
        .expect_err("the CREATE TABLE rolled back with the block");
    assert_eq!(code(&error), "42P01", "{}", message(&error));
}

#[tokio::test]
async fn conditions_and_assignments_evaluate_through_the_sql_engine_with_bound_variables() {
    let server = pg_server::start_server(0).await;
    let client = pg_server::connect(server.port).await;
    client
        .batch_execute(
            "DO $$ DECLARE op text := 'INSERT'; label text; BEGIN \
               IF (op = 'INSERT') THEN label := 'is ' || op; ELSE label := 'no'; END IF; \
               IF label = 'is INSERT' THEN \
                 INSERT INTO ks.t (id, ck, name, score) VALUES ('w1', 1, label, 1); \
               END IF; END $$",
        )
        .await
        .expect("DO evaluates `$1 = 'INSERT'` and `'is ' || $1`");
    let rows = client
        .query("SELECT name FROM ks.t WHERE id = 'w1'", &[])
        .await
        .expect("read");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "is INSERT");
}

#[tokio::test]
async fn json_build_object_over_a_variable_assigns_a_json_value() {
    let server = pg_server::start_server(0).await;
    let client = pg_server::connect(server.port).await;
    client
        .batch_execute(
            "DO $$ DECLARE q text := 'default'; notice json; label text; BEGIN \
               notice = json_build_object('queue', q); \
               label = notice::text; \
               INSERT INTO ks.t (id, ck, name, score) VALUES ('j', 1, label, 1); END $$",
        )
        .await
        .expect("json_build_object evaluates");
    let rows = client
        .query("SELECT name FROM ks.t WHERE id = 'j'", &[])
        .await
        .expect("read");
    assert_eq!(rows[0].get::<_, String>(0), "{\"queue\": \"default\"}");
}

/// Awaits W1/W3: the census DO block verbatim. Today its guard stops at
/// `'public'::regnamespace` (`0A000`, cast not supported); after that it needs `CREATE TYPE …
/// AS ENUM` (W3) and transactional DDL inside the block's transaction (W3).
#[cfg(feature = "pending-workstreams")]
#[tokio::test]
async fn awaits_w1_w3_the_census_do_block_creates_the_enum() {
    let server = pg_server::start_server_with_ddl().await;
    let client = pg_server::connect(server.port).await;
    let census = "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'oban_job_state' AND typnamespace = 'public'::regnamespace::oid) THEN CREATE TYPE \"public\".oban_job_state AS ENUM ( 'available', 'suspended', 'scheduled', 'executing', 'retryable', 'completed', 'discarded', 'cancelled' ); END IF; END$$;;";
    client
        .batch_execute(census)
        .await
        .expect("the census DO block runs");
    client
        .batch_execute(census)
        .await
        .expect("and is idempotent");
}
