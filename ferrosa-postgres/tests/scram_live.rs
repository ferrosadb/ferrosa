//! Live SCRAM handshake driven by a real Postgres driver (`tokio-postgres`),
//! in-process over loopback — no external infrastructure required. This
//! exercises the full random-nonce SCRAM-SHA-256 exchange and the run-time
//! parameter / ReadyForQuery sequence that the RFC-vector unit tests cannot.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ferrosa_postgres::handshake::VerifierStore;
use ferrosa_postgres::scram::ScramVerifier;
use ferrosa_postgres::{server, QueryContext};
use ferrosa_schema::{
    AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
    RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
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

fn schema_config() -> SchemaConfig {
    SchemaConfig {
        hasher: PasswordHasher::Bcrypt { cost: 4 },
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    }
}

fn engine_config(dir: &Path) -> StorageEngineConfig {
    StorageEngineConfig {
        commit_log: CommitLogConfig {
            segment_size: 256 * 1024,
            max_segment_age: Duration::from_secs(60),
            sync_strategy: SyncStrategyConfig::Batch,
            batch: Default::default(),
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 4096,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 5,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        write_verify: false,
    }
}

/// A minimal `QueryContext` over a temp engine and bare schema. The returned
/// `TempDir` guard must outlive the server (its `Drop` removes the data dir).
fn minimal_ctx() -> (Arc<QueryContext>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
    let schema = Schema::new(schema_config()).expect("schema bootstraps");
    let ctx = Arc::new(QueryContext {
        engine: Arc::new(engine),
        schema: Arc::new(schema),
        default_schema: "public".into(),
    });
    (ctx, dir)
}

async fn spawn_server(store: Arc<OneRole>, ctx: Arc<QueryContext>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(server::serve(listener, store, ctx));
    port
}

#[tokio::test]
async fn real_driver_scram_then_select1_succeeds_and_txn_fails_loud() {
    let (ctx, _dir) = minimal_ctx();
    let port = spawn_server(dev_store(), ctx).await;

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

    // Auth succeeded and the session reached ReadyForQuery. No-FROM expression
    // SELECTs are now supported: `SELECT 1` returns one row with value 1.
    let msgs = client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT 1 should succeed");
    let one = msgs
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("expected a data row from SELECT 1");
    assert_eq!(one.get(0), Some("1"));

    // `version()` is evaluated from the connection context.
    let vmsgs = client
        .simple_query("SELECT version()")
        .await
        .expect("version() should succeed");
    assert!(
        vmsgs.iter().any(|m| matches!(
            m,
            tokio_postgres::SimpleQueryMessage::Row(r)
                if r.get(0).unwrap_or_default().contains("ferrosa")
        )),
        "version() should report a ferrosa version string"
    );

    // Transactions route through Accord and are not yet wired: `BEGIN` must
    // fail loud (SQLSTATE 0A000, feature_not_supported) — never a fake success.
    let err = client
        .simple_query("BEGIN")
        .await
        .expect_err("transactions are not yet implemented and must fail loud");
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
    let (ctx, _dir) = minimal_ctx();
    let port = spawn_server(dev_store(), ctx).await;

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
