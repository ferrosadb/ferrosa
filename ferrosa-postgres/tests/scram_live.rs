//! Live SCRAM handshake driven by a real Postgres driver (`tokio-postgres`),
//! in-process over loopback — no external infrastructure required. This
//! exercises the full random-nonce SCRAM-SHA-256 exchange and the run-time
//! parameter / ReadyForQuery sequence that the RFC-vector unit tests cannot.

use std::sync::Arc;

use ferrosa_postgres::handshake::VerifierStore;
use ferrosa_postgres::scram::ScramVerifier;
use ferrosa_postgres::server;
use tokio::net::TcpListener;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Config, NoTls};

struct OneRole {
    user: String,
    verifier: ScramVerifier,
}

impl VerifierStore for OneRole {
    fn verifier(&self, user: &str) -> Option<ScramVerifier> {
        (user == self.user).then(|| self.verifier.clone())
    }
}

fn dev_store() -> Arc<OneRole> {
    let salt = b"ferrosa-dev-salt";
    Arc::new(OneRole {
        user: "ferrosa_user".into(),
        verifier: ScramVerifier::from_password("devpass", salt, 4096),
    })
}

async fn spawn_server(store: Arc<OneRole>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(server::serve(listener, store));
    port
}

#[tokio::test]
async fn real_driver_completes_scram_then_query_fails_loud() {
    let port = spawn_server(dev_store()).await;

    // tokio-postgres performs a real SCRAM-SHA-256 exchange with random nonces.
    let (client, connection) = Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("ferrosa_user")
        .password("devpass")
        .dbname("ferrosa")
        .ssl_mode(SslMode::Disable)
        .connect(NoTls)
        .await
        .expect("SCRAM handshake should succeed against ferrosa-postgres");
    let conn_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    // Auth succeeded and the session reached ReadyForQuery. The query itself
    // fails loud (no relational engine yet) with SQLSTATE 0A000.
    let err = client
        .simple_query("SELECT 1")
        .await
        .expect_err("query should fail loud until the engine lands");
    assert_eq!(
        err.code().map(|c| c.code()),
        Some("0A000"),
        "unexpected error: {err}"
    );

    drop(client);
    let _ = conn_task.await;
}

#[tokio::test]
async fn wrong_password_is_rejected_by_real_driver() {
    let port = spawn_server(dev_store()).await;

    let result = Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("ferrosa_user")
        .password("WRONG")
        .dbname("ferrosa")
        .ssl_mode(SslMode::Disable)
        .connect(NoTls)
        .await;

    assert!(result.is_err(), "wrong password must not authenticate");
}
