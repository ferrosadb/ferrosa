//! The unowned-node posture of the PostgreSQL front-end (ADR-024, forge
//! t_f8ca9e8f).
//!
//! A node with `auth_enabled=false` seeds no roles and replays none from
//! `system_auth`, so no role on it carries a SCRAM verifier. Before this
//! decision every login against such a node was answered `FATAL:
//! Handshake(UnknownRole)` — an internal enum name that names neither the cause
//! nor the fix, on a listener whose whole job was to say "the box is a
//! database".
//!
//! These tests drive a real driver (`tokio-postgres`) against the real listener
//! over loopback and pin the two halves of the decision:
//!
//! 1. On the unowned node the client is refused at the protocol level with the
//!    named message and SQLSTATE `28000` (an authorization-configuration
//!    failure, not a wrong password). The assertion is on the message text, not
//!    on "the connection failed".
//! 2. The SAME listener — no restart — serves `SELECT 1` as soon as a login role
//!    with a verifier exists, which is what the appliance's psql row needs
//!    (auth enabled + `ferrosa_admin` seeded).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ferrosa_postgres::{server, AccordAccess, QueryContext, SchemaVerifierStore, NO_ROLES_REFUSAL};
use ferrosa_schema::auth::role::RoleMetadata;
use ferrosa_schema::{
    AuthContext, AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
    RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use tokio::net::TcpListener;
use tokio_postgres::config::SslMode;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Config, NoTls};

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
            sync_strategy: SyncStrategyConfig::Periodic {
                sync_interval: Duration::from_secs(1),
            },
            batch: Default::default(),
            log_dir: dir.join("commitlog"),
            checkpoint_dir: dir.join("commitlog"),
            archive: None,
        },
        compaction: CompactionConfig::from_env(dir.join("compaction")),
        object_store: None,
        local_cache_max_bytes: 1024 * 1024,
        local_disk_free_reserve_bytes: 0,
        flush_threshold_bytes: 64 * 1024 * 1024,
        memtable_backpressure_bytes: u64::MAX,
        flush_max_age_secs: 3600,
        data_dir: dir.to_path_buf(),
        index_backend: ferrosa_storage::index::IndexBackendConfig::Local,
        auth_enabled: false,
        auth_warn: false,
        max_pending_replay_mutations_without_schema: 1024,
        memtable_num_shards: 64,
        cache_hot_window_secs: 900,
        write_verify: false,
    }
}

/// A live listener over the SAME schema registry the store reads: exactly the
/// node's wiring (`ferrosa/src/main.rs` step 11b), with `auth_enabled=false` —
/// no role was seeded and none was replayed.
struct Listener {
    port: u16,
    schema: Arc<Schema>,
    _engine: Arc<StorageEngine>,
    _dir: tempfile::TempDir,
}

async fn start_unowned_node() -> Listener {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine =
        Arc::new(StorageEngine::new(engine_config(dir.path()), None).expect("storage engine"));
    let schema = Arc::new(Schema::new(schema_config()).expect("schema bootstraps"));
    assert!(
        !SchemaVerifierStore::new(Arc::clone(&schema)).login_roles_configured(),
        "precondition: an auth-disabled node holds no login role"
    );
    let ctx = Arc::new(QueryContext {
        cluster_read: Default::default(),
        engine: Arc::clone(&engine),
        schema: Arc::clone(&schema),
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord: AccordAccess::disabled(),
        ddl: None,
        jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
        portals: Arc::new(ferrosa_postgres::SuspendedPortals::new(
            ferrosa_postgres::PortalLimits::default(),
        )),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let store = Arc::new(SchemaVerifierStore::new(Arc::clone(&schema)));
    tokio::spawn(server::serve(
        listener,
        store,
        ctx,
        server::PgTls::plaintext(),
    ));
    Listener {
        port,
        schema,
        _engine: engine,
        _dir: dir,
    }
}

fn client_config(port: u16, user: &str, password: &str) -> Config {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user(user)
        .password(password)
        .dbname("ferrosa")
        .ssl_mode(SslMode::Disable);
    config
}

/// Every login against a node with no roles is refused BY NAME: the client log
/// carries the operator's next step, not `Handshake(UnknownRole)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_with_no_roles_refuses_every_login_by_name() {
    let node = start_unowned_node().await;

    let error = client_config(node.port, "ferrosa_admin", "ferrosa_admin")
        .connect(NoTls)
        .await
        .err()
        .expect("an unowned node can authenticate nobody");

    let db_error = error
        .as_db_error()
        .unwrap_or_else(|| panic!("expected a protocol-level FATAL, got: {error}"));
    // Asserted by name, not by "connection failed": the text is the fix.
    assert_eq!(
        db_error.message(),
        NO_ROLES_REFUSAL,
        "the refusal names the cause and the keys that fix it"
    );
    assert!(
        db_error.message().contains("FERROSA_AUTH_ENABLED=true"),
        "the message names the config key that turns on role bootstrap"
    );
    assert!(
        db_error.message().contains("FERROSA_SEED_ADMIN_PASSWORD"),
        "the message names the key that sets the administrator password"
    );
    assert_eq!(
        *db_error.code(),
        SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
        "a node with no roles is an authorization-configuration failure (28000), \
         not a wrong password (28P01)"
    );

    // An unknown *user* on the same unowned node gets the same answer: the
    // refusal is about the node, not about who asked.
    let error = client_config(node.port, "nobody", "whatever")
        .connect(NoTls)
        .await
        .err()
        .expect("no role exists to authenticate as");
    assert_eq!(
        error
            .as_db_error()
            .expect("protocol-level refusal")
            .message(),
        NO_ROLES_REFUSAL
    );
}

/// The appliance's psql row: the SAME listener serves `SELECT 1` once a login
/// role with a verifier exists. Adding the role is what `auth_enabled=true`
/// does at boot (seed or `system_auth` replay); nothing about the listener
/// changes, so the posture cannot regress the row that must stay satisfiable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_listener_serves_select_1_once_a_role_exists() {
    let node = start_unowned_node().await;

    let admin = AuthContext {
        role: "ferrosa_admin".to_string(),
        is_superuser: true,
        must_change_password: false,
    };
    node.schema
        .create_role(
            RoleMetadata {
                name: "ferrosa_admin".to_string(),
                is_superuser: true,
                can_login: true,
                salted_hash: None,
                member_of: Default::default(),
                scram: None,
            },
            Some("first-boot-password"),
            &admin,
        )
        .expect("create a login role with a derived SCRAM verifier");
    assert!(
        SchemaVerifierStore::new(Arc::clone(&node.schema)).login_roles_configured(),
        "the role just created is a configured login role"
    );

    let (client, connection) = client_config(node.port, "ferrosa_admin", "first-boot-password")
        .connect(NoTls)
        .await
        .expect("a role with a verifier authenticates over the same listener");
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("driver connection ended: {error}");
        }
    });

    let messages = client
        .simple_query("SELECT 1 AS one")
        .await
        .expect("SELECT 1 is served");
    let value = messages
        .iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get("one"),
            _ => None,
        })
        .expect("SELECT 1 AS one returns a row");
    assert_eq!(value, "1");
}
