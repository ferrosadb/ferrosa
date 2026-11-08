//! PL/pgSQL functions and AFTER ROW triggers over the wire (D13).
//!
//! The server runs functions on the interpreter backend (`Procedural::interpreted()`); that
//! the compiled backend agrees with it is `ferrosa-udf/tests/plpgsql_compiled.rs` and
//! `specs/lean4/Ferrosa/Sql/Plpgsql.lean`.

#[path = "common/pg_server.rs"]
mod pg_server;

use std::time::Duration;

use ferrosa_postgres::procedural::Procedural;
use futures::{stream, StreamExt};
use tokio::sync::mpsc;
use tokio_postgres::config::SslMode;
use tokio_postgres::{AsyncMessage, Config, NoTls};

async fn server() -> pg_server::TestServer {
    pg_server::start_server_with_procedural(Procedural::interpreted()).await
}

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
        while let Some(Ok(message)) = messages.next().await {
            if let AsyncMessage::Notification(n) = message {
                let _ = tx.send((n.channel().to_string(), n.payload().to_string()));
            }
        }
    });
    (client, rx)
}

async fn next(rx: &mut mpsc::UnboundedReceiver<(String, String)>) -> String {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a notification arrives")
        .expect("open")
        .1
}

async fn quiet(rx: &mut mpsc::UnboundedReceiver<(String, String)>) {
    if let Ok(Some(n)) = tokio::time::timeout(Duration::from_millis(300), rx.recv()).await {
        panic!("unexpected notification {n:?}");
    }
}

fn code(error: &tokio_postgres::Error) -> String {
    error
        .code()
        .map(|c| c.code().to_string())
        .unwrap_or_else(|| format!("no SQLSTATE: {error}"))
}

async fn run(client: &tokio_postgres::Client, statements: &[&str]) {
    for sql in statements {
        client
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
}

const NOTIFY_NAME: &str = "CREATE FUNCTION notify_name() RETURNS trigger AS $$ BEGIN PERFORM pg_notify('rows', NEW.name); RETURN NULL; END $$ LANGUAGE plpgsql";
const ON_INSERT: &str =
    "CREATE TRIGGER on_insert AFTER INSERT ON t FOR EACH ROW EXECUTE PROCEDURE notify_name()";

#[tokio::test]
async fn an_insert_trigger_fires_once_per_row_in_row_order() {
    let server = server().await;
    let (a, mut rx) = listener(server.port).await;
    run(&a, &["LISTEN rows"]).await;
    let b = pg_server::connect(server.port).await;
    run(&b, &[NOTIFY_NAME, ON_INSERT]).await;
    run(
        &b,
        &["INSERT INTO t (id, ck, name, score) VALUES ('a', 1, 'first', 1), ('a', 2, 'second', 2), ('b', 1, 'third', 3)"],
    )
    .await;
    assert_eq!(next(&mut rx).await, "first");
    assert_eq!(next(&mut rx).await, "second");
    assert_eq!(next(&mut rx).await, "third");
    quiet(&mut rx).await;
}

#[tokio::test]
async fn a_trigger_fires_for_an_extended_protocol_insert() {
    let server = server().await;
    let (a, mut rx) = listener(server.port).await;
    run(&a, &["LISTEN rows"]).await;
    let b = pg_server::connect(server.port).await;
    run(&b, &[NOTIFY_NAME, ON_INSERT]).await;
    b.execute(
        "INSERT INTO t (id, ck, name, score) VALUES ($1, $2, $3, $4)",
        &[&"x", &1i32, &"bound", &1i32],
    )
    .await
    .expect("extended insert");
    assert_eq!(next(&mut rx).await, "bound");
}

const AUDIT_TABLE: &str = "CREATE TABLE audit_log (ck int PRIMARY KEY, name text)";
const AUDIT: &str = "CREATE FUNCTION audit() RETURNS trigger AS $$ BEGIN INSERT INTO audit_log (ck, name) VALUES (NEW.ck, NEW.name); RETURN NULL; END $$ LANGUAGE plpgsql";
const ON_INSERT_AUDIT: &str =
    "CREATE TRIGGER audit_insert AFTER INSERT ON t FOR EACH ROW EXECUTE PROCEDURE audit()";

async fn audit_rows(client: &tokio_postgres::Client) -> usize {
    client
        .query("SELECT name FROM audit_log", &[])
        .await
        .expect("audit query")
        .len()
}

#[tokio::test]
async fn trigger_writes_join_the_transaction_and_roll_back_with_it() {
    let server = server().await;
    let c = pg_server::connect(server.port).await;
    run(&c, &[AUDIT_TABLE, AUDIT, ON_INSERT_AUDIT]).await;
    run(
        &c,
        &[
            "BEGIN",
            "INSERT INTO t (id, ck, name, score) VALUES ('base', 7, 'kept?', 1)",
            "ROLLBACK",
        ],
    )
    .await;
    assert_eq!(
        audit_rows(&c).await,
        0,
        "the trigger's write rolled back with the statement's"
    );
    run(
        &c,
        &[
            "BEGIN",
            "INSERT INTO t (id, ck, name, score) VALUES ('base', 8, 'kept', 1)",
            "COMMIT",
        ],
    )
    .await;
    assert_eq!(audit_rows(&c).await, 1, "and committed with it");
}

#[tokio::test]
async fn update_of_a_column_fires_only_when_that_column_is_set() {
    let server = server().await;
    let (a, mut rx) = listener(server.port).await;
    run(&a, &["LISTEN rows"]).await;
    let b = pg_server::connect(server.port).await;
    run(
        &b,
        &[
            "CREATE FUNCTION renamed() RETURNS trigger AS $$ BEGIN PERFORM pg_notify('rows', OLD.name || '->' || NEW.name); RETURN NULL; END $$ LANGUAGE plpgsql",
            "CREATE TRIGGER on_rename AFTER UPDATE OF name ON t FOR EACH ROW EXECUTE PROCEDURE renamed()",
            "INSERT INTO t (id, ck, name, score) VALUES ('u', 1, 'old', 1)",
            "UPDATE t SET score = 5 WHERE id = 'u' AND ck = 1",
        ],
    )
    .await;
    quiet(&mut rx).await;
    run(&b, &["UPDATE t SET name = 'new' WHERE id = 'u' AND ck = 1"]).await;
    assert_eq!(next(&mut rx).await, "old->new");
}

#[tokio::test]
async fn a_failing_trigger_fails_its_statement_and_writes_nothing() {
    let server = server().await;
    let c = pg_server::connect(server.port).await;
    run(
        &c,
        &[
            "CREATE FUNCTION broken() RETURNS trigger AS $$ BEGIN INSERT INTO missing_table (id) VALUES (NEW.id); RETURN NULL; END $$ LANGUAGE plpgsql",
            "CREATE TRIGGER breaks AFTER INSERT ON t FOR EACH ROW EXECUTE PROCEDURE broken()",
        ],
    )
    .await;
    let error = c
        .batch_execute("INSERT INTO t (id, ck, name, score) VALUES ('f', 1, 'x', 1)")
        .await
        .expect_err("the trigger fails");
    assert_eq!(code(&error), "42P01");
    let rows = c
        .query("SELECT name FROM t WHERE id = 'f'", &[])
        .await
        .expect("read");
    assert!(rows.is_empty(), "the row went with the failed statement");
}

#[tokio::test]
async fn drop_trigger_and_drop_function_follow_postgresql_dependencies() {
    let server = server().await;
    let (a, mut rx) = listener(server.port).await;
    run(&a, &["LISTEN rows"]).await;
    let b = pg_server::connect(server.port).await;
    run(&b, &[NOTIFY_NAME, ON_INSERT]).await;
    let error = b
        .batch_execute("DROP FUNCTION notify_name()")
        .await
        .expect_err("a trigger depends on it");
    assert_eq!(code(&error), "2BP01");
    run(
        &b,
        &[
            "DROP TRIGGER on_insert ON t",
            "DROP TRIGGER IF EXISTS on_insert ON t",
            "INSERT INTO t (id, ck, name, score) VALUES ('d', 1, 'silent', 1)",
        ],
    )
    .await;
    quiet(&mut rx).await;
    run(
        &b,
        &[
            "DROP FUNCTION notify_name()",
            "DROP FUNCTION IF EXISTS notify_name()",
        ],
    )
    .await;
    let error = b
        .batch_execute("DROP FUNCTION notify_name()")
        .await
        .expect_err("gone");
    assert_eq!(code(&error), "42883");
    let error = b
        .batch_execute("DROP TRIGGER on_insert ON t")
        .await
        .expect_err("gone");
    assert_eq!(code(&error), "42704");
}

#[tokio::test]
async fn function_and_trigger_ddl_errors_have_postgresql_sqlstates() {
    let server = server().await;
    let c = pg_server::connect(server.port).await;
    let refused = c
        .batch_execute("CREATE FUNCTION f() RETURNS trigger AS $$ BEGIN RAISE NOTICE 'x'; RETURN NULL; END $$ LANGUAGE plpgsql")
        .await
        .expect_err("RAISE");
    assert_eq!(code(&refused), "0A000");
    let missing = c
        .batch_execute("CREATE TRIGGER t1 AFTER INSERT ON t FOR EACH ROW EXECUTE PROCEDURE f()")
        .await
        .expect_err("the refused function was not stored");
    assert_eq!(code(&missing), "42883");
    run(
        &c,
        &["CREATE FUNCTION scalar_fn(v int) RETURNS int AS $$ BEGIN RETURN v; END $$ LANGUAGE plpgsql"],
    )
    .await;
    let dup = c
        .batch_execute("CREATE FUNCTION scalar_fn(v int) RETURNS int AS $$ BEGIN RETURN v; END $$ LANGUAGE plpgsql")
        .await
        .expect_err("exists");
    assert_eq!(code(&dup), "42723");
    run(
        &c,
        &["CREATE OR REPLACE FUNCTION scalar_fn(v int) RETURNS int AS $$ BEGIN RETURN v; END $$ LANGUAGE plpgsql"],
    )
    .await;
    let not_trigger = c
        .batch_execute(
            "CREATE FUNCTION nt() RETURNS int AS $$ BEGIN RETURN 1; END $$ LANGUAGE plpgsql",
        )
        .await;
    not_trigger.expect("a scalar function is fine on its own");
    let wrong = c
        .batch_execute("CREATE TRIGGER t2 AFTER INSERT ON t FOR EACH ROW EXECUTE PROCEDURE nt()")
        .await
        .expect_err("not a trigger function");
    assert_eq!(
        code(&wrong),
        "42P17",
        "PostgreSQL: function must return type trigger"
    );
    let no_table = c
        .batch_execute(&ON_INSERT.replace(" ON t ", " ON nowhere "))
        .await
        .expect_err("no table");
    assert_eq!(code(&no_table), "42P01");
}

#[tokio::test]
async fn or_replace_changes_what_the_trigger_runs() {
    let server = server().await;
    let (a, mut rx) = listener(server.port).await;
    run(&a, &["LISTEN rows"]).await;
    let b = pg_server::connect(server.port).await;
    run(&b, &[NOTIFY_NAME, ON_INSERT]).await;
    run(
        &b,
        &[
            "CREATE OR REPLACE FUNCTION notify_name() RETURNS trigger AS $$ BEGIN PERFORM pg_notify('rows', 'replaced'); RETURN NULL; END $$ LANGUAGE plpgsql",
            "INSERT INTO t (id, ck, name, score) VALUES ('r', 1, 'ignored', 1)",
        ],
    )
    .await;
    assert_eq!(next(&mut rx).await, "replaced");
}

#[tokio::test]
async fn without_a_function_backend_create_function_is_refused() {
    let server = pg_server::start_server_with_ddl().await;
    let c = pg_server::connect(server.port).await;
    let error = c.batch_execute(NOTIFY_NAME).await.expect_err("no backend");
    assert_eq!(code(&error), "0A000");
}

#[tokio::test]
async fn function_ddl_inside_a_transaction_is_refused_like_other_ddl() {
    let server = server().await;
    let c = pg_server::connect(server.port).await;
    run(&c, &["BEGIN"]).await;
    let error = c
        .batch_execute(NOTIFY_NAME)
        .await
        .expect_err("in a transaction");
    assert_eq!(code(&error), "25001");
}

/// Oban's v1 notify trigger, with `public` read as this server's schema and `state` a text
/// column (enum types are W3's). Exercises TG_OP, NEW/OLD, json_build_object, AT TIME ZONE
/// and pg_notify through the SQL engine.
#[tokio::test]
async fn the_oban_v1_notify_trigger_notifies_on_insert_and_on_state_change() {
    let server = server().await;
    let (a, mut rx) = listener(server.port).await;
    run(
        &a,
        &[
            "LISTEN \"public.oban_insert\"",
            "LISTEN \"public.oban_update\"",
        ],
    )
    .await;
    let b = pg_server::connect(server.port).await;
    run(
        &b,
        &[
            "CREATE TABLE oban_jobs (id bigint PRIMARY KEY, state text, queue text, scheduled_at timestamp)",
            "CREATE OR REPLACE FUNCTION oban_jobs_notify() RETURNS trigger AS $$
DECLARE
  channel text;
  notice json;
BEGIN
  IF (TG_OP = 'INSERT') THEN
    channel = 'public.oban_insert';
    notice = json_build_object('queue', NEW.queue, 'state', NEW.state);

    -- No point triggering for a job that isn't scheduled to run now
    IF NEW.scheduled_at IS NOT NULL AND NEW.scheduled_at > now() AT TIME ZONE 'utc' THEN
      RETURN null;
    END IF;
  ELSE
    channel = 'public.oban_update';
    notice = json_build_object('queue', NEW.queue, 'new_state', NEW.state, 'old_state', OLD.state);
  END IF;

  PERFORM pg_notify(channel, notice::text);

  RETURN NULL;
END;
$$ LANGUAGE plpgsql",
            "CREATE TRIGGER oban_notify AFTER INSERT OR UPDATE OF state ON oban_jobs FOR EACH ROW EXECUTE PROCEDURE oban_jobs_notify()",
        ],
    )
    .await;
    let insert = "INSERT INTO oban_jobs (id, state, queue, scheduled_at) VALUES ($1, $2, $3, $4)";
    let past = std::time::UNIX_EPOCH + Duration::from_secs(946_684_800); // 2000-01-01
    let future = std::time::UNIX_EPOCH + Duration::from_secs(32_503_680_000); // 3000-01-01
    b.execute(insert, &[&1i64, &"available", &"default", &past])
        .await
        .expect("insert a job due now");
    assert_eq!(
        next(&mut rx).await,
        "{\"queue\": \"default\", \"state\": \"available\"}"
    );
    b.execute(insert, &[&2i64, &"scheduled", &"default", &future])
        .await
        .expect("insert a job scheduled later");
    quiet(&mut rx).await;
    run(
        &b,
        &["UPDATE oban_jobs SET state = 'executing' WHERE id = 1"],
    )
    .await;
    assert_eq!(
        next(&mut rx).await,
        "{\"queue\": \"default\", \"new_state\": \"executing\", \"old_state\": \"available\"}"
    );
}

#[tokio::test]
async fn a_trigger_that_fires_itself_stops_at_the_depth_bound() {
    let server = server().await;
    let c = pg_server::connect(server.port).await;
    run(
        &c,
        &[
            "CREATE FUNCTION again() RETURNS trigger AS $$ BEGIN INSERT INTO t (id, ck, name, score) VALUES (NEW.id, NEW.ck, NEW.name, 0); RETURN NULL; END $$ LANGUAGE plpgsql",
            "CREATE TRIGGER again AFTER INSERT ON t FOR EACH ROW EXECUTE PROCEDURE again()",
        ],
    )
    .await;
    let error = c
        .batch_execute("INSERT INTO t (id, ck, name, score) VALUES ('loop', 1, 'x', 1)")
        .await
        .expect_err("the cascade is bounded");
    assert_eq!(code(&error), "54001");
    let rows = c
        .query("SELECT ck FROM t WHERE id = 'loop'", &[])
        .await
        .expect("read");
    assert!(
        rows.is_empty(),
        "the whole cascade rolled back with the statement"
    );
}

/// D13 end to end on the compiled backend: CREATE FUNCTION compiles the body to a component
/// and the trigger runs it. Needs the AssemblyScript bundle:
/// `FERROSA_ASC_BUNDLE=… cargo test -p ferrosa-postgres --features 'plpgsql-compile live-infra-tests' --test pg_triggers compiled`
#[cfg(all(feature = "plpgsql-compile", feature = "live-infra-tests"))]
#[tokio::test]
async fn compiled_trigger_function_fires_and_notifies() {
    assert!(
        std::env::var_os("FERROSA_ASC_BUNDLE").is_some(),
        "FERROSA_ASC_BUNDLE is not set: build it with ferrosa-udf/examples/asc-poc/build-bundle.sh"
    );
    let executor = std::sync::Arc::new(
        ferrosa_udf::UdfExecutor::new(ferrosa_udf::SandboxConfig::default()).expect("executor"),
    );
    let server = pg_server::start_server_with_procedural(Procedural::new(
        ferrosa_postgres::procedural::FunctionBackend::Compiled(executor),
    ))
    .await;
    let (a, mut rx) = listener(server.port).await;
    run(&a, &["LISTEN rows"]).await;
    let b = pg_server::connect(server.port).await;
    run(
        &b,
        &[NOTIFY_NAME, ON_INSERT, AUDIT_TABLE, AUDIT, ON_INSERT_AUDIT],
    )
    .await;
    run(
        &b,
        &["INSERT INTO t (id, ck, name, score) VALUES ('c', 1, 'compiled', 1), ('c', 2, 'twice', 1)"],
    )
    .await;
    assert_eq!(next(&mut rx).await, "compiled");
    assert_eq!(next(&mut rx).await, "twice");
    assert_eq!(
        audit_rows(&b).await,
        2,
        "the compiled audit trigger wrote in the same transaction"
    );
}
