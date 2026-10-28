//! t_9fba4678: successful PostgreSQL DDL must make pre-existing rows usable
//! immediately through the new index. SELECT may use the MVCC complete-row
//! path; UPDATE and the storage tracker establish index readiness here.
//!
//! CREATE INDEX is currently rejected by the SQL parser; ADD PRIMARY KEY is
//! pgbench's public entry into ClusterDdl::create_index. Direct, pair and Raft
//! apply all call build_replicated_index, which queues ordinary-column backfill.
//! This exercises that shared production path without external infrastructure.

#[path = "common/pg_server.rs"]
mod pg_server;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use async_trait::async_trait;
use ferrosa_cluster::ddl_path::DdlPath;
use ferrosa_postgres::handshake::VerifierStore;
use ferrosa_postgres::scram::ScramVerifier;
use ferrosa_postgres::{server, AccordAccess, ClusterDdl, DdlExecutor, QueryContext};
use ferrosa_schema::{
    AuthContext, AuthMethod, DeploymentMode, EnvSecretsProvider, KeyspaceMetadata, PasswordHasher,
    PasswordPolicy, RateLimitConfig, ReplicationParams, Schema, SchemaConfig, TableMetadata,
    TableUpdates, TestAuditSink,
};
use ferrosa_storage::index::IndexBackendConfig;
use ferrosa_storage::{StorageEngine, StorageEngineConfig, TableId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_postgres::Client;

#[derive(Default)]
struct BuildGate(Mutex<Option<oneshot::Sender<()>>>);

impl BuildGate {
    fn release(&self) {
        if let Some(sender) = self.0.lock().unwrap().take() {
            let _ = sender.send(());
        }
    }
}

impl Drop for BuildGate {
    fn drop(&mut self) {
        self.release();
    }
}

/// Delegate every operation/result to production. Only control when the
/// external builder can finish: a real readiness wait yields Pending, at which
/// point we release it. An early Ready keeps backfill held through the client
/// reads. No elapsed-time assertion or scheduler race decides the result.
struct GatedDdl {
    inner: ClusterDdl,
    gate: Arc<BuildGate>,
}

#[async_trait]
impl DdlExecutor for GatedDdl {
    async fn create_table(&self, table: TableMetadata) -> Result<(), String> {
        self.inner.create_table(table).await
    }

    async fn drop_table(&self, keyspace: &str, table: &str) -> Result<(), String> {
        self.inner.drop_table(keyspace, table).await
    }

    async fn alter_table(
        &self,
        keyspace: &str,
        table: &str,
        updates: TableUpdates,
    ) -> Result<(), String> {
        self.inner.alter_table(keyspace, table, updates).await
    }

    async fn create_index(
        &self,
        keyspace: &str,
        table: &str,
        name: &str,
        columns: &[String],
    ) -> Result<(), String> {
        let mut operation = self.inner.create_index(keyspace, table, name, columns);
        futures::future::poll_fn(|cx| {
            let result = operation.as_mut().poll(cx);
            if matches!(result, Poll::Pending) {
                self.gate.release();
            }
            result
        })
        .await
    }
}

struct TestRole(ScramVerifier);

impl VerifierStore for TestRole {
    fn verifier(&self, user: &str) -> Option<ScramVerifier> {
        (user == "ferrosa_user").then(|| self.0.clone())
    }
    fn admit(&self, _: &str) -> Result<(), String> {
        Ok(())
    }
    fn record_failure(&self, _: &str) {}
    fn record_success(&self, user: &str) -> Result<AuthContext, String> {
        Ok(AuthContext {
            role: user.into(),
            is_superuser: true,
            must_change_password: false,
        })
    }
}

struct Fixture {
    client: Client,
    engine: Arc<StorageEngine>,
    gate: Arc<BuildGate>,
    path: Arc<arc_swap::ArcSwap<DdlPath>>,
    listener: tokio::task::JoinHandle<std::io::Result<()>>,
    builder: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.gate.release();
        self.listener.abort();
        self.builder.abort();
    }
}

#[derive(Clone, Copy)]
enum BuildOutcome {
    Complete,
    Fail,
    Deferred,
}

async fn fixture() -> Fixture {
    fixture_with(BuildOutcome::Complete).await
}

async fn fixture_with(outcome: BuildOutcome) -> Fixture {
    let builder_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", builder_listener.local_addr().unwrap());
    let (release, held) = oneshot::channel();
    let gate = Arc::new(BuildGate(Mutex::new(Some(release))));
    let builder = tokio::spawn(async move {
        let _ = held.await;
        // A transient remote failure invokes the real LocalBackend, which
        // builds and installs real sidecars. Never fabricate index contents
        // or mark the readiness tracker current from test code.
        let (mut socket, _) = builder_listener.accept().await.unwrap();
        let mut request = [0; 4096];
        assert!(socket.read(&mut request).await.unwrap() > 0);
        let response = match outcome {
            BuildOutcome::Complete | BuildOutcome::Deferred => {
                "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_string()
            }
            BuildOutcome::Fail => {
                let body =
                    r#"{"status":"failed","error":"deliberate permanent index build failure"}"#;
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
            }
        };
        socket.write_all(response.as_bytes()).await.unwrap();
    });

    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageEngineConfig::test_config(dir.path());
    config.flush_threshold_bytes = 64 * 1024 * 1024;
    config.compaction.min_threshold = 50;
    config.index_backend = IndexBackendConfig::Remote {
        endpoints: vec![endpoint],
        // Longer than the whole-test hang guard, so timeout cannot release
        // backfill during the observation and accidentally turn RED green.
        timeout: Duration::from_secs(120),
        max_retries: 0,
        circuit_breaker_threshold: 1,
        circuit_breaker_recovery: Duration::from_secs(120),
    };
    if matches!(outcome, BuildOutcome::Deferred) {
        config.index_backend = IndexBackendConfig::Off;
    }
    let engine = Arc::new(StorageEngine::new(config, None).unwrap());
    engine
        .register_system_tables()
        .expect("register DDL persistence tables");
    let schema = Arc::new(
        Schema::new(SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        })
        .unwrap(),
    );
    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: "public".into(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".into(),
                    options: HashMap::from([("replication_factor".into(), "1".into())]),
                },
            },
            &AuthContext {
                role: "cassandra".into(),
                is_superuser: true,
                must_change_password: false,
            },
        )
        .unwrap();
    let path = Arc::new(arc_swap::ArcSwap::from_pointee(DdlPath::Direct {
        schema: Arc::clone(&schema),
        engine: Arc::clone(&engine),
    }));
    let context = Arc::new(QueryContext {
        cluster_read: Default::default(),
        engine: Arc::clone(&engine),
        schema,
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord: AccordAccess::disabled(),
        ddl: Some(Arc::new(GatedDdl {
            inner: ClusterDdl::new(Arc::clone(&path)),
            gate: Arc::clone(&gate),
        })),
        jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
        portals: Arc::new(ferrosa_postgres::SuspendedPortals::new(Default::default())),
    });
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let listener = tokio::spawn(server::serve(
        socket,
        Arc::new(TestRole(ScramVerifier::from_password(
            "devpass",
            b"ferrosa-dev-salt",
            4096,
        ))),
        context,
        server::PgTls::plaintext(),
    ));
    Fixture {
        client: pg_server::connect(port).await,
        engine,
        gate,
        path,
        listener,
        builder,
        _dir: dir,
    }
}

async fn indexed_rows(client: &Client) -> Result<Vec<(i32, i32)>, tokio_postgres::Error> {
    let mut values = Vec::new();
    for aid in 1..=3 {
        let rows = client
            .query(
                &format!("SELECT aid, abalance FROM pgbench_accounts WHERE aid = {aid}"),
                &[],
            )
            .await?;
        values.extend(rows.iter().map(|row| (row.get(0), row.get(1))));
    }
    Ok(values)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_primary_key_success_makes_flushed_rows_immediately_readable_and_updatable() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let fixture = fixture().await;
        let client = &fixture.client;
        client
            .batch_execute("CREATE TABLE pgbench_accounts (aid integer, abalance integer)")
            .await
            .expect("create pgbench's heap table");
        client
            .batch_execute("INSERT INTO pgbench_accounts (aid, abalance) VALUES (1, 100), (2, 200), (3, 300)")
            .await
            .expect("populate the heap before declaring its primary key");
        let table = TableId::new("public", "pgbench_accounts");
        fixture.engine.flush(&table).expect("persist pre-index rows");
        assert!(fixture.engine.sstable_count(&table) > 0);
        assert_eq!(indexed_rows(client).await.unwrap(), [(1, 100), (2, 200), (3, 300)]);

        client
            .batch_execute("ALTER TABLE pgbench_accounts ADD PRIMARY KEY (aid)")
            .await
            .expect("ordinary PostgreSQL DDL must complete successfully");
        assert!(fixture.engine.declares_index(&table, "pgbench_accounts_pkey"));
        let current_at_success = fixture.engine.index_is_current(&table, "pgbench_accounts_pkey");

        // Capture BOTH public operations before releasing the held build, so
        // an early DDL success cannot win a race against a fast local builder.
        let before = indexed_rows(client).await;
        let updated = client
            .execute("UPDATE pgbench_accounts SET abalance = abalance + 7 WHERE aid = 2", &[])
            .await;
        let after = indexed_rows(client).await;
        fixture.gate.release();
        eprintln!("index current at DDL success: {current_at_success}; immediate SELECT: {before:?}; immediate UPDATE: {updated:?}");

        assert_eq!(
            before.expect("successful ADD PRIMARY KEY must permit complete immediate reads"),
            [(1, 100), (2, 200), (3, 300)]
        );
        assert_eq!(updated.expect("immediate indexed UPDATE must succeed"), 1);
        assert_eq!(
            after.expect("all rows remain readable after indexed UPDATE"),
            [(1, 100), (2, 207), (3, 300)]
        );
        assert!(current_at_success, "DDL success must mean its index is current");
    })
    .await
    .expect("DDL/readiness regression exceeded its hang guard");
}

async fn populate(fixture: &Fixture) -> TableId {
    fixture
        .client
        .batch_execute("CREATE TABLE pgbench_accounts (aid integer, abalance integer)")
        .await
        .unwrap();
    fixture
        .client
        .batch_execute(
            "INSERT INTO pgbench_accounts (aid, abalance) VALUES (1, 100), (2, 200), (3, 300)",
        )
        .await
        .unwrap();
    let table = TableId::new("public", "pgbench_accounts");
    fixture.engine.flush(&table).unwrap();
    table
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn permanent_build_failure_reaches_the_sql_client() {
    let fixture = fixture_with(BuildOutcome::Fail).await;
    let table = populate(&fixture).await;
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        fixture
            .client
            .batch_execute("ALTER TABLE pgbench_accounts ADD PRIMARY KEY (aid)"),
    )
    .await
    .expect("failure must be reported promptly")
    .expect_err("failed backfill cannot acknowledge DDL");
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "58000");
    assert!(
        error
            .message()
            .contains("deliberate permanent index build failure"),
        "{error:?}"
    );
    assert!(!fixture
        .engine
        .index_is_current(&table, "pgbench_accounts_pkey"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deferred_build_has_a_bounded_readiness_deadline() {
    let fixture = fixture_with(BuildOutcome::Deferred).await;
    let table = populate(&fixture).await;
    let error = tokio::time::timeout(
        Duration::from_secs(35),
        fixture
            .client
            .batch_execute("ALTER TABLE pgbench_accounts ADD PRIMARY KEY (aid)"),
    )
    .await
    .expect("deferred backfill must have an internal deadline")
    .expect_err("deferred is not ready");
    assert!(
        error.as_db_error().unwrap().message().contains("timed out"),
        "{error:?}"
    );
    assert!(!fixture
        .engine
        .index_is_current(&table, "pgbench_accounts_pkey"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_readiness_keeps_pending_postings_unreadable() {
    let fixture = fixture().await;
    let table = populate(&fixture).await;
    let ddl = ClusterDdl::new(Arc::clone(&fixture.path));
    let columns = vec!["aid".into()];
    ddl.alter_table(
        "public",
        "pgbench_accounts",
        TableUpdates {
            params: None,
            add_columns: Vec::new(),
            drop_columns: Vec::new(),
            extensions: Some(HashMap::from([(
                ferrosa_postgres::pg_key::PRIMARY_KEY_EXTENSION.into(),
                ferrosa_postgres::pg_key::encode(&columns),
            )])),
        },
    )
    .await
    .unwrap();
    let mut operation = ddl.create_index(
        "public",
        "pgbench_accounts",
        "pgbench_accounts_pkey",
        &columns,
    );
    assert!(
        matches!(futures::poll!(&mut operation), Poll::Pending),
        "backfill must be awaited"
    );
    drop(operation); // Cancels the caller's wait; durable build work is not rolled back.
    assert!(fixture
        .engine
        .declares_index(&table, "pgbench_accounts_pkey"));
    assert!(!fixture
        .engine
        .index_is_current(&table, "pgbench_accounts_pkey"));
    let error = fixture
        .client
        .execute(
            "UPDATE pgbench_accounts SET abalance = 7 WHERE aid = 2",
            &[],
        )
        .await
        .expect_err("guard must still reject missing postings after cancellation");
    assert_eq!(error.code().unwrap().code(), "58000");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_tracker_is_an_error_not_ready() {
    let fixture = fixture().await;
    populate(&fixture).await;
    let ddl = ClusterDdl::new(Arc::clone(&fixture.path));
    let columns = vec!["aid".into()];
    let mut operation = ddl.create_index("public", "pgbench_accounts", "idx", &columns);
    assert!(matches!(futures::poll!(&mut operation), Poll::Pending));
    assert!(fixture
        .engine
        .index_tracker()
        .remove_index("public", "pgbench_accounts", "idx"));
    let error = tokio::time::timeout(Duration::from_secs(2), operation)
        .await
        .unwrap()
        .expect_err("missing tracker must fail");
    assert!(error.contains("not registered"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_table_generation_cannot_complete_an_old_wait() {
    let fixture = fixture().await;
    populate(&fixture).await;
    let ddl = ClusterDdl::new(Arc::clone(&fixture.path));
    let columns = vec!["aid".into()];
    let mut operation = ddl.create_index("public", "pgbench_accounts", "idx", &columns);
    assert!(matches!(futures::poll!(&mut operation), Poll::Pending));
    fixture
        .client
        .batch_execute("DROP TABLE pgbench_accounts")
        .await
        .unwrap();
    fixture
        .client
        .batch_execute("CREATE TABLE pgbench_accounts (aid integer, abalance integer)")
        .await
        .unwrap();
    // A new, empty incarnation can have a current index under the same name.
    ddl.create_index("public", "pgbench_accounts", "idx", &columns)
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), operation)
        .await
        .unwrap()
        .expect_err("replacement generation cannot satisfy old DDL");
    assert!(error.contains("retired"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_sstable_does_not_hide_pending_replacement() {
    let fixture = fixture().await;
    let table = populate(&fixture).await;
    let ddl = ClusterDdl::new(Arc::clone(&fixture.path));
    let columns = vec!["aid".into()];
    let mut operation = ddl.create_index("public", "pgbench_accounts", "idx", &columns);
    assert!(matches!(futures::poll!(&mut operation), Poll::Pending));
    let tracker = fixture.engine.index_tracker();
    let old = tracker
        .get_state("public", "pgbench_accounts", "idx")
        .unwrap()
        .pending_sstables
        .into_iter()
        .collect::<Vec<_>>();
    assert!(!old.is_empty());
    // Model the tracker notification from compaction, without fabricating
    // current postings: its replacement still needs an actual build.
    tracker.retire_sstables("public", "pgbench_accounts", &old, Some("replacement"));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut operation)
            .await
            .is_err()
    );
    assert!(!fixture.engine.index_is_current(&table, "idx"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deployment_transition_cannot_acknowledge_local_readiness() {
    let fixture = fixture().await;
    populate(&fixture).await;
    let ddl = ClusterDdl::new(Arc::clone(&fixture.path));
    let columns = vec!["aid".into()];
    let mut operation = ddl.create_index("public", "pgbench_accounts", "idx", &columns);
    assert!(matches!(futures::poll!(&mut operation), Poll::Pending));
    fixture.path.store(Arc::new(DdlPath::Unavailable));
    let error = tokio::time::timeout(Duration::from_secs(2), operation)
        .await
        .unwrap()
        .expect_err("mode transition invalidates local readiness");
    assert!(error.contains("changed"), "{error}");
}

struct NoPeers;
impl ferrosa_net::peer::PeerEventListener for NoPeers {
    fn on_peer_connected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
    fn on_peer_disconnected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
    fn on_peer_suspected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
    fn on_peer_recovered(&self, _: uuid::Uuid) {}
    fn on_peer_failed(&self, _: uuid::Uuid) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pair_mode_refuses_index_readiness_before_local_creation() {
    use ferrosa_cluster::pair::{ddl::DdlCoordinator, PairRole};
    let fixture = fixture().await;
    let table = populate(&fixture).await;
    let original = fixture.path.load_full();
    let DdlPath::Direct { schema, engine } = original.as_ref() else {
        panic!("fixture must be Direct");
    };
    let peers = Arc::new(ferrosa_net::peer::PeerManager::new(
        Arc::new(ferrosa_net::config::NetConfig::default()),
        uuid::Uuid::new_v4(),
        Arc::new(NoPeers),
    ));
    fixture
        .path
        .store(Arc::new(DdlPath::Pair(Arc::new(DdlCoordinator::new(
            Arc::new(arc_swap::ArcSwap::from_pointee(PairRole::Primary)),
            uuid::Uuid::new_v4(),
            Arc::clone(schema),
            Arc::clone(engine),
            peers,
        )))));
    let ddl = ClusterDdl::new(Arc::clone(&fixture.path));
    let error = ddl
        .create_index("public", "pgbench_accounts", "idx", &["aid".into()])
        .await
        .expect_err("local apply cannot prove peer readiness");
    assert!(error.contains("replica index readiness"), "{error}");
    assert!(!fixture.engine.declares_index(&table, "idx"));
}
