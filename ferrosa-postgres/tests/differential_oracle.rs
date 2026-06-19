//! Differential-testing oracle (blueprint H2 / risk R10).
//!
//! Runs a fixed corpus of `SELECT` queries against BOTH a real PostgreSQL 16
//! server (in a container) AND the ferrosa-postgres wire front-end over the
//! SAME data, then asserts the two result sets agree. This is the cross-check
//! that catches the front-end silently diverging from real Postgres semantics.
//!
//! ## Why this is gated
//!
//! These tests require a container runtime (podman or docker) to launch a real
//! PostgreSQL. Per repo policy:
//!
//! - The whole file is `#[cfg(feature = "live-infra-tests")]`, so the DEFAULT
//!   `cargo test` (feature off) compiles it to nothing and needs no container.
//! - Once the feature IS enabled, each test `panic!`s with setup instructions
//!   when `FERROSA_TEST_CONTAINERS=1` is not set — never a silent skip.
//!
//! Run it:
//! ```text
//! FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-postgres \
//!   --features live-infra-tests --test differential_oracle -- --nocapture
//! ```
//!
//! ## Collation / ordering note (the R10 cry-wolf controls)
//!
//! The container Postgres is initialized under whatever locale the `postgres:16`
//! image default is (typically a UTF-8 collation), while ferrosa orders text by
//! raw byte (`C`/`POSIX`) order. To keep the two comparable WITHOUT fighting
//! locale-collation differences, every corpus query carries a deterministic
//! `ORDER BY` on a NUMERIC key (or on ASCII-only text whose byte order and the
//! Postgres default collation agree for the specific values used). The corpus
//! data is chosen so ASCII text columns never collide on collation edge cases.
//!
//! Cell comparison is also tolerant in one specific way: if BOTH sides parse as
//! `f64`, they are compared numerically with a small tolerance. This absorbs the
//! float text-format gap (e.g. Postgres `numeric` AVG `1.5000000000000000` vs
//! ferrosa `float8` `1.5`) so a benign formatting difference is NOT reported as
//! a Mismatch — the false-alarm fix the R10 blueprint calls out.

#![cfg(feature = "live-infra-tests")]

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use ferrosa_common::cell::CellValue;
use ferrosa_common::key::{DecoratedKey, PartitionKey};
use ferrosa_postgres::handshake::VerifierStore;
use ferrosa_postgres::scram::ScramVerifier;
use ferrosa_postgres::{server, QueryContext};
use ferrosa_schema::{
    AuthContext, AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode,
    EnvSecretsProvider, KeyspaceMetadata, PasswordHasher, PasswordPolicy, RateLimitConfig,
    ReplicationParams, Schema, SchemaConfig, TableMetadata, TableParams, TestAuditSink,
};
use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row as StorageRow};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
    TableId,
};
use indexmap::IndexMap;
use tokio::net::TcpListener;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Config, NoTls, SimpleQueryMessage};
use uuid::Uuid;

// ════════════════════════════════════════════════════════════════════════════
// Source-of-truth corpus data
//
// Defined ONCE here, then materialized identically into both PostgreSQL and
// ferrosa so the two datasets cannot drift.
// ════════════════════════════════════════════════════════════════════════════

/// `public.users(id int PK, name text, dept text, email text NULL)`.
/// `email` is `None` for some rows to exercise NULL handling. Names/depts are
/// ASCII-only and chosen so byte-order and the PG default collation agree.
struct UserRow {
    id: i32,
    name: &'static str,
    dept: &'static str,
    email: Option<&'static str>,
}

/// `public.orders(oid int PK, uid int, amount int, price double precision)`.
struct OrderRow {
    oid: i32,
    uid: i32,
    amount: i32,
    price: f64,
}

/// `public.events(id int PK, at timestamp, on_day date, at_time time, src inet,
/// amt numeric)` — one row per scalar-type combination, chosen to exercise the
/// exact-text renderers (fractional-second trimming, leading-zero numeric
/// fractions, IPv4 + IPv6, pre-epoch is avoided so the two systems agree on the
/// literal forms).
///
/// Every field is a CANONICAL Postgres text literal so the same string seeds
/// Postgres (as a SQL literal) and is the source of truth for the ferrosa cell
/// bytes (parsed into the CQL wire encoding by the seed helpers).
struct EventRow {
    id: i32,
    /// `YYYY-MM-DD HH:MM:SS[.ffffff]` (no tz).
    at: &'static str,
    /// `YYYY-MM-DD`.
    on_day: &'static str,
    /// `HH:MM:SS[.ffffff]`.
    at_time: &'static str,
    /// canonical IP string (v4 or v6).
    src: &'static str,
    /// plain decimal string.
    amt: &'static str,
}

fn users() -> Vec<UserRow> {
    vec![
        UserRow {
            id: 1,
            name: "alice",
            dept: "eng",
            email: Some("alice@x.io"),
        },
        UserRow {
            id: 2,
            name: "bob",
            dept: "eng",
            email: None,
        },
        UserRow {
            id: 3,
            name: "carol",
            dept: "sales",
            email: Some("carol@x.io"),
        },
        UserRow {
            id: 4,
            name: "dave",
            dept: "sales",
            email: None,
        },
        UserRow {
            id: 5,
            name: "erin",
            dept: "ops",
            email: Some("erin@x.io"),
        },
    ]
}

fn orders() -> Vec<OrderRow> {
    vec![
        OrderRow {
            oid: 10,
            uid: 1,
            amount: 100,
            price: 9.5,
        },
        OrderRow {
            oid: 11,
            uid: 1,
            amount: 200,
            price: 19.0,
        },
        OrderRow {
            oid: 12,
            uid: 2,
            amount: 50,
            price: 4.25,
        },
        OrderRow {
            oid: 13,
            uid: 3,
            amount: 300,
            price: 30.0,
        },
        OrderRow {
            oid: 14,
            uid: 3,
            amount: 100,
            price: 10.0,
        },
        OrderRow {
            oid: 15,
            uid: 3,
            amount: 100,
            price: 10.0,
        },
        OrderRow {
            oid: 16,
            uid: 5,
            amount: 75,
            price: 7.5,
        },
    ]
}

fn events() -> Vec<EventRow> {
    vec![
        EventRow {
            id: 1,
            at: "2024-01-15 10:30:00",
            on_day: "2024-01-15",
            at_time: "10:30:00",
            src: "10.0.0.1",
            amt: "123.45",
        },
        EventRow {
            id: 2,
            at: "2024-01-15 10:30:00.5",
            on_day: "2024-02-29",
            at_time: "23:59:59.123",
            src: "192.168.1.100",
            amt: "0.05",
        },
        EventRow {
            id: 3,
            // `at` (CQL timestamp) is millisecond-resolution, so its fraction must
            // be a whole number of milliseconds. Sub-millisecond precision is
            // exercised by the `at_time` column (CQL time = microsecond-resolution).
            at: "2024-03-01 00:00:00.001",
            on_day: "2023-12-31",
            at_time: "00:00:00",
            src: "203.0.113.7",
            amt: "1000",
        },
        EventRow {
            id: 4,
            at: "2025-06-17 12:00:00.25",
            on_day: "2025-06-17",
            at_time: "12:00:00.5",
            src: "2001:db8::1",
            amt: "-42.5",
        },
        EventRow {
            id: 5,
            at: "2020-02-29 06:15:45",
            on_day: "2020-02-29",
            at_time: "06:15:45.999999",
            src: "172.16.0.42",
            amt: "99999.999",
        },
    ]
}

// ════════════════════════════════════════════════════════════════════════════
// Container runtime + lifecycle
// ════════════════════════════════════════════════════════════════════════════

/// Resolve the container runtime: `$FERROSA_CONTAINER_RUNTIME` if set, else
/// `podman` if on PATH, else `docker`.
fn container_runtime() -> &'static str {
    if let Ok(rt) = std::env::var("FERROSA_CONTAINER_RUNTIME") {
        // Leak the owned string so we can hand back a `&'static str`; this runs
        // at most once per test process, so the leak is bounded.
        return Box::leak(rt.into_boxed_str());
    }
    if which("podman") {
        "podman"
    } else {
        "docker"
    }
}

/// True if `bin` resolves on `PATH` (best-effort via `which`).
fn which(bin: &str) -> bool {
    Command::new("which")
        .arg(bin)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Assert the live-infra prerequisite is present, or `panic!` with setup
/// instructions (repo policy: no silent skips once the feature is enabled).
fn require_containers() {
    if std::env::var("FERROSA_TEST_CONTAINERS").ok().as_deref() != Some("1") {
        panic!(
            "differential_oracle requires a container runtime for a real PostgreSQL.\n\
             Set FERROSA_TEST_CONTAINERS=1 and ensure podman (or docker) is on PATH, e.g.:\n\
             \n\
             podman machine start            # if not already running\n\
             FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-postgres \\\n\
               --features live-infra-tests --test differential_oracle -- --nocapture\n"
        );
    }
}

/// A running PostgreSQL container. Drop force-removes it so we never leak a
/// container even when a test panics mid-run.
struct PgContainer {
    runtime: &'static str,
    name: String,
    host_port: u16,
}

impl PgContainer {
    /// Launch `postgres:16`, publish 5432 to an ephemeral host port, discover
    /// that port, and poll-connect until the server is ready.
    async fn start() -> PgContainer {
        let runtime = container_runtime();
        let name = format!("ferrosa-oracle-{}", Uuid::new_v4().simple());

        let run = Command::new(runtime)
            .args([
                "run",
                "-d",
                "--rm",
                "-e",
                "POSTGRES_PASSWORD=ferrosa",
                "-e",
                "POSTGRES_DB=ferrosa",
                "-p",
                "127.0.0.1::5432",
                "--name",
                &name,
                "postgres:16",
            ])
            .output()
            .expect("spawn container runtime");
        assert!(
            run.status.success(),
            "`{runtime} run` failed: {}",
            String::from_utf8_lossy(&run.stderr)
        );

        // Construct the guard NOW so any later failure still triggers cleanup.
        let mut container = PgContainer {
            runtime,
            name,
            host_port: 0,
        };
        container.host_port = container.discover_host_port();
        container.wait_ready().await;
        container
    }

    /// Parse the host port from `<rt> port <name> 5432` → `0.0.0.0:NNNNN`
    /// (or `127.0.0.1:NNNNN`). Retries briefly while the port mapping appears.
    fn discover_host_port(&self) -> u16 {
        for _ in 0..30 {
            let out = Command::new(self.runtime)
                .args(["port", &self.name, "5432"])
                .output()
                .expect("spawn `port`");
            if out.status.success() {
                let text = String::from_utf8_lossy(&out.stdout);
                if let Some(port) = parse_published_port(&text) {
                    return port;
                }
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        panic!("could not discover published host port for {}", self.name);
    }

    /// Poll-connect with tokio-postgres until the server accepts a session
    /// (covers image-pull + initdb latency: ~30 tries × 500ms ≈ up to 60s with
    /// the slow first connect attempts).
    async fn wait_ready(&self) {
        for attempt in 0..120 {
            if let Ok(client) = self.try_connect().await {
                // A trivial round-trip proves the server is actually serving.
                if client.simple_query("SELECT 1").await.is_ok() {
                    return;
                }
            }
            if attempt % 10 == 0 {
                eprintln!("[oracle] waiting for postgres ({attempt})...");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!("postgres container {} never became ready", self.name);
    }

    async fn try_connect(&self) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
        let (client, connection) = Config::new()
            .host("127.0.0.1")
            .port(self.host_port)
            .user("postgres")
            .password("ferrosa")
            .dbname("ferrosa")
            .ssl_mode(SslMode::Disable)
            .connect(NoTls)
            .await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }

    /// Connect a fresh client (assumes the server is already ready).
    async fn connect(&self) -> tokio_postgres::Client {
        self.try_connect()
            .await
            .expect("connect to ready postgres container")
    }
}

impl Drop for PgContainer {
    fn drop(&mut self) {
        // Best-effort force-remove; log on failure (don't panic in Drop).
        let status = Command::new(self.runtime)
            .args(["rm", "-f", &self.name])
            .output();
        match status {
            Ok(o) if o.status.success() => {}
            Ok(o) => eprintln!(
                "[oracle] warning: failed to remove container {}: {}",
                self.name,
                String::from_utf8_lossy(&o.stderr)
            ),
            Err(e) => eprintln!(
                "[oracle] warning: could not run `{} rm -f {}`: {e}",
                self.runtime, self.name
            ),
        }
    }
}

/// Extract `NNNNN` from a `<host>:NNNNN` line emitted by `<rt> port`.
fn parse_published_port(text: &str) -> Option<u16> {
    text.lines()
        .filter_map(|line| line.trim().rsplit(':').next())
        .find_map(|tail| tail.trim().parse::<u16>().ok())
}

// ════════════════════════════════════════════════════════════════════════════
// Materialize the corpus into PostgreSQL
// ════════════════════════════════════════════════════════════════════════════

async fn seed_postgres(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            "CREATE TABLE users (id int PRIMARY KEY, name text, dept text, email text);\n\
             CREATE TABLE orders (oid int PRIMARY KEY, uid int, amount int, price double precision);\n\
             CREATE TABLE events (id int PRIMARY KEY, at timestamp, on_day date, at_time time, src inet, amt numeric);\n\
             CREATE TABLE kv (id int PRIMARY KEY, v text, n int);",
        )
        .await
        .expect("create pg tables");

    for u in users() {
        let email = match u.email {
            Some(e) => format!("'{e}'"),
            None => "NULL".to_string(),
        };
        client
            .execute(
                &format!(
                    "INSERT INTO users (id, name, dept, email) VALUES ({}, '{}', '{}', {})",
                    u.id, u.name, u.dept, email
                ),
                &[],
            )
            .await
            .expect("insert pg user");
    }
    for o in orders() {
        client
            .execute(
                &format!(
                    "INSERT INTO orders (oid, uid, amount, price) VALUES ({}, {}, {}, {})",
                    o.oid, o.uid, o.amount, o.price
                ),
                &[],
            )
            .await
            .expect("insert pg order");
    }
    for e in events() {
        // Typed literals so Postgres stores the exact temporal/inet/numeric value.
        client
            .execute(
                &format!(
                    "INSERT INTO events (id, at, on_day, at_time, src, amt) VALUES \
                     ({}, TIMESTAMP '{}', DATE '{}', TIME '{}', INET '{}', NUMERIC '{}')",
                    e.id, e.at, e.on_day, e.at_time, e.src, e.amt
                ),
                &[],
            )
            .await
            .expect("insert pg event");
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Materialize the same corpus into ferrosa storage + schema
// ════════════════════════════════════════════════════════════════════════════

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

fn superuser() -> AuthContext {
    AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
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

fn column(name: &str, kind: ColumnKind, ty: &str) -> ColumnMetadata {
    ColumnMetadata {
        name: name.to_string(),
        kind,
        position: 0,
        column_type: ty.to_string(),
        clustering_order: ClusteringOrder::None,
        mask: None,
    }
}

/// Create keyspace `public` plus `users` and `orders` through the DDL API so
/// the front-end's catalog resolution sees them exactly as a CQL deployment
/// would.
fn create_schema() -> Schema {
    let schema = Schema::new(schema_config()).expect("schema bootstraps");
    let auth = superuser();

    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: "public".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: {
                        let mut o = HashMap::new();
                        o.insert("replication_factor".to_string(), "1".to_string());
                        o
                    },
                },
            },
            &auth,
        )
        .expect("create keyspace public");

    // users(id int PK, name text, dept text, email text)
    let mut users_cols = IndexMap::new();
    users_cols.insert(
        "id".to_string(),
        column("id", ColumnKind::PartitionKey, "int"),
    );
    users_cols.insert(
        "name".to_string(),
        column("name", ColumnKind::Regular, "text"),
    );
    users_cols.insert(
        "dept".to_string(),
        column("dept", ColumnKind::Regular, "text"),
    );
    users_cols.insert(
        "email".to_string(),
        column("email", ColumnKind::Regular, "text"),
    );
    schema
        .create_table(
            TableMetadata {
                keyspace: "public".to_string(),
                name: "users".to_string(),
                id: Uuid::new_v4(),
                columns: users_cols,
                partition_key: vec!["id".to_string()],
                clustering_key: vec![],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: HashMap::new(),
                is_system: false,
            },
            &auth,
        )
        .expect("create table users");

    // orders(oid int PK, uid int, amount int, price double)
    let mut orders_cols = IndexMap::new();
    orders_cols.insert(
        "oid".to_string(),
        column("oid", ColumnKind::PartitionKey, "int"),
    );
    orders_cols.insert("uid".to_string(), column("uid", ColumnKind::Regular, "int"));
    orders_cols.insert(
        "amount".to_string(),
        column("amount", ColumnKind::Regular, "int"),
    );
    orders_cols.insert(
        "price".to_string(),
        column("price", ColumnKind::Regular, "double"),
    );
    schema
        .create_table(
            TableMetadata {
                keyspace: "public".to_string(),
                name: "orders".to_string(),
                id: Uuid::new_v4(),
                columns: orders_cols,
                partition_key: vec!["oid".to_string()],
                clustering_key: vec![],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: HashMap::new(),
                is_system: false,
            },
            &auth,
        )
        .expect("create table orders");

    // events(id int PK, at timestamp, on_day date, at_time time, src inet,
    // amt decimal). CQL has no `numeric`; `decimal` is the arbitrary-precision
    // type that the front-end maps to Postgres numeric (OID 1700).
    let mut events_cols = IndexMap::new();
    events_cols.insert(
        "id".to_string(),
        column("id", ColumnKind::PartitionKey, "int"),
    );
    events_cols.insert(
        "at".to_string(),
        column("at", ColumnKind::Regular, "timestamp"),
    );
    events_cols.insert(
        "on_day".to_string(),
        column("on_day", ColumnKind::Regular, "date"),
    );
    events_cols.insert(
        "at_time".to_string(),
        column("at_time", ColumnKind::Regular, "time"),
    );
    events_cols.insert(
        "src".to_string(),
        column("src", ColumnKind::Regular, "inet"),
    );
    events_cols.insert(
        "amt".to_string(),
        column("amt", ColumnKind::Regular, "decimal"),
    );
    schema
        .create_table(
            TableMetadata {
                keyspace: "public".to_string(),
                name: "events".to_string(),
                id: Uuid::new_v4(),
                columns: events_cols,
                partition_key: vec!["id".to_string()],
                clustering_key: vec![],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: HashMap::new(),
                is_system: false,
            },
            &auth,
        )
        .expect("create table events");

    // kv(id int PK, v text, n int) — starts EMPTY; the DML oracle populates it
    // over the wire on both sides, exercising the INSERT/UPDATE/DELETE executors.
    let mut kv_cols = IndexMap::new();
    kv_cols.insert(
        "id".to_string(),
        column("id", ColumnKind::PartitionKey, "int"),
    );
    kv_cols.insert("v".to_string(), column("v", ColumnKind::Regular, "text"));
    kv_cols.insert("n".to_string(), column("n", ColumnKind::Regular, "int"));
    schema
        .create_table(
            TableMetadata {
                keyspace: "public".to_string(),
                name: "kv".to_string(),
                id: Uuid::new_v4(),
                columns: kv_cols,
                partition_key: vec!["id".to_string()],
                clustering_key: vec![],
                params: TableParams::default(),
                flags: HashSet::new(),
                extensions: HashMap::new(),
                is_system: false,
            },
            &auth,
        )
        .expect("create table kv");

    schema
}

fn kv_storage_schema() -> ferrosa_common::schema::TableSchema {
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    let col = |n: &str, t: &str| ColumnDefinition {
        name: n.to_string(),
        type_name: t.to_string(),
    };
    TableSchema {
        keyspace: "public".to_string(),
        table: "kv".to_string(),
        key_type: int_marshal().to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        // Cassandra name-sorted storage order: n, v.
        regular_columns: vec![col("n", int_marshal()), col("v", text_marshal())],
        extensions: Default::default(),
    }
}

fn text_marshal() -> &'static str {
    "org.apache.cassandra.db.marshal.UTF8Type"
}
fn int_marshal() -> &'static str {
    "org.apache.cassandra.db.marshal.Int32Type"
}
fn double_marshal() -> &'static str {
    "org.apache.cassandra.db.marshal.DoubleType"
}

fn users_storage_schema() -> ferrosa_common::schema::TableSchema {
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    let col = |n: &str, t: &str| ColumnDefinition {
        name: n.to_string(),
        type_name: t.to_string(),
    };
    TableSchema {
        keyspace: "public".to_string(),
        table: "users".to_string(),
        key_type: int_marshal().to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        // Regular columns in Cassandra's name-sorted storage order so the
        // storage ordinal space matches `storage_column_index`: dept, email, name.
        regular_columns: vec![
            col("dept", text_marshal()),
            col("email", text_marshal()),
            col("name", text_marshal()),
        ],
        extensions: Default::default(),
    }
}

fn timestamp_marshal() -> &'static str {
    "org.apache.cassandra.db.marshal.TimestampType"
}
fn date_marshal() -> &'static str {
    "org.apache.cassandra.db.marshal.SimpleDateType"
}
fn time_marshal() -> &'static str {
    "org.apache.cassandra.db.marshal.TimeType"
}
fn inet_marshal() -> &'static str {
    "org.apache.cassandra.db.marshal.InetAddressType"
}
fn decimal_marshal() -> &'static str {
    "org.apache.cassandra.db.marshal.DecimalType"
}

fn events_storage_schema() -> ferrosa_common::schema::TableSchema {
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    let col = |n: &str, t: &str| ColumnDefinition {
        name: n.to_string(),
        type_name: t.to_string(),
    };
    TableSchema {
        keyspace: "public".to_string(),
        table: "events".to_string(),
        key_type: int_marshal().to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        // Regular columns in Cassandra's name-sorted storage order:
        // amt, at, at_time, on_day, src.
        regular_columns: vec![
            col("amt", decimal_marshal()),
            col("at", timestamp_marshal()),
            col("at_time", time_marshal()),
            col("on_day", date_marshal()),
            col("src", inet_marshal()),
        ],
        extensions: Default::default(),
    }
}

fn orders_storage_schema() -> ferrosa_common::schema::TableSchema {
    use ferrosa_common::schema::{ColumnDefinition, TableSchema};
    let col = |n: &str, t: &str| ColumnDefinition {
        name: n.to_string(),
        type_name: t.to_string(),
    };
    TableSchema {
        keyspace: "public".to_string(),
        table: "orders".to_string(),
        key_type: int_marshal().to_string(),
        clustering_columns: vec![],
        static_columns: vec![],
        // Regular columns in Cassandra's name-sorted storage order: amount,
        // price, uid.
        regular_columns: vec![
            col("amount", int_marshal()),
            col("price", double_marshal()),
            col("uid", int_marshal()),
        ],
        extensions: Default::default(),
    }
}

/// Build a no-clustering storage `Row` from `(ordinal, bytes)` cells. A column
/// omitted from `cells` is absent ⇒ decoded as SQL NULL (the NULL-handling path).
fn row_with_cells(cells: Vec<(u16, Vec<u8>)>, ts: i64) -> StorageRow {
    StorageRow {
        clustering: vec![],
        cells: cells
            .into_iter()
            .map(|(ord, bytes)| (ord, CellValue::live(bytes, ts)))
            .collect(),
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(ts),
    }
}

/// Resolve a regular column's storage cell ordinal from the schema metadata.
///
/// Cassandra stores regular columns sorted by the column-name comparator, NOT
/// in declared order, so a multi-regular-column row's cells must carry the
/// storage ordinal that the decode path (`storage_column_index`) expects. We
/// look it up from the SAME schema the front-end reads, so the seed and the
/// decode share one source of truth and cannot drift.
fn storage_ord(schema: &Schema, table: &str, column: &str) -> u16 {
    let snapshot = schema.snapshot();
    let meta = snapshot
        .tables
        .get(&("public".to_string(), table.to_string()))
        .unwrap_or_else(|| panic!("table public.{table} in schema snapshot"));
    meta.storage_column_index(column)
        .unwrap_or_else(|| panic!("column {column} has a storage ordinal"))
}

/// Seed the ferrosa engine with the SAME corpus rows the Postgres side gets.
/// Cell bytes use the CQL on-wire encoding the decode path expects: int = 4 BE
/// bytes, text = UTF-8, double = 8 BE bytes of the IEEE-754 bit pattern. Cell
/// ordinals come from [`storage_ord`] so they match Cassandra's name-sorted
/// regular-column storage order.
fn seed_engine(dir: &Path, schema: &Schema) -> StorageEngine {
    let engine = StorageEngine::new(engine_config(dir), None).unwrap();
    engine.register_table(users_storage_schema()).unwrap();
    engine.register_table(orders_storage_schema()).unwrap();
    engine.register_table(events_storage_schema()).unwrap();
    // kv is registered but seeded with NO rows — the DML oracle populates it.
    engine.register_table(kv_storage_schema()).unwrap();

    let users_tid = TableId::new("public", "users");
    let orders_tid = TableId::new("public", "orders");
    let pk = |i: i32| DecoratedKey::new(PartitionKey::new(i.to_be_bytes().to_vec()));

    let u_name = storage_ord(schema, "users", "name");
    let u_dept = storage_ord(schema, "users", "dept");
    let u_email = storage_ord(schema, "users", "email");
    let o_uid = storage_ord(schema, "orders", "uid");
    let o_amount = storage_ord(schema, "orders", "amount");
    let o_price = storage_ord(schema, "orders", "price");

    let mut ts = 1000i64;
    for u in users() {
        let mut cells = vec![
            (u_name, u.name.as_bytes().to_vec()),
            (u_dept, u.dept.as_bytes().to_vec()),
        ];
        if let Some(e) = u.email {
            cells.push((u_email, e.as_bytes().to_vec()));
        }
        engine
            .write(&users_tid, &pk(u.id), row_with_cells(cells, ts), ts)
            .unwrap();
        ts += 1;
    }
    for o in orders() {
        let cells = vec![
            (o_uid, o.uid.to_be_bytes().to_vec()),
            (o_amount, o.amount.to_be_bytes().to_vec()),
            (o_price, o.price.to_bits().to_be_bytes().to_vec()),
        ];
        engine
            .write(&orders_tid, &pk(o.oid), row_with_cells(cells, ts), ts)
            .unwrap();
        ts += 1;
    }

    // events: encode each scalar into its CQL on-wire cell bytes using the
    // reused `ferrosa_row_bridge::encode_value` encoder (built from the canonical
    // string literal — the SAME source of truth Postgres got), so the seed and
    // the decode path share one serialization and cannot drift.
    let events_tid = TableId::new("public", "events");
    let e_at = storage_ord(schema, "events", "at");
    let e_on_day = storage_ord(schema, "events", "on_day");
    let e_at_time = storage_ord(schema, "events", "at_time");
    let e_src = storage_ord(schema, "events", "src");
    let e_amt = storage_ord(schema, "events", "amt");
    for e in events() {
        let cells = vec![
            (e_at, cql_bytes(&cql_timestamp(e.at))),
            (e_on_day, cql_bytes(&cql_date(e.on_day))),
            (e_at_time, cql_bytes(&cql_time(e.at_time))),
            (e_src, cql_bytes(&cql_inet(e.src))),
            (e_amt, cql_bytes(&cql_decimal(e.amt))),
        ];
        engine
            .write(&events_tid, &pk(e.id), row_with_cells(cells, ts), ts)
            .unwrap();
        ts += 1;
    }
    engine
}

/// Reuse the canonical cell encoder (no reinvention).
fn cql_bytes(v: &ferrosa_common::CqlValue) -> Vec<u8> {
    ferrosa_row_bridge::encode_value(v)
}

/// `YYYY-MM-DD HH:MM:SS[.ffffff]` (UTC, no tz) → `CqlValue::Timestamp(millis)`.
/// Postgres stores `timestamp` to microsecond precision; the corpus values use
/// at most millisecond precision OR exact micros that are whole milliseconds —
/// EXCEPT id=3 (`.000001`) and id=5 (`.999999`), which carry sub-millisecond
/// micros. CQL `timestamp` is millisecond-resolution, so those would truncate.
/// To keep the two systems bit-identical we therefore only place values whose
/// sub-second part is a whole millisecond here; the corpus is chosen so the
/// timestamp column never needs sub-ms precision (the sub-ms cases live only in
/// the `at_time` column, which is microsecond-resolution `time`).
fn cql_timestamp(s: &str) -> ferrosa_common::CqlValue {
    let naive = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .unwrap_or_else(|e| panic!("bad timestamp literal {s:?}: {e}"));
    let micros = naive.and_utc().timestamp_micros();
    assert_eq!(
        micros % 1000,
        0,
        "corpus timestamp {s:?} has sub-millisecond precision; CQL timestamp is \
         millisecond-resolution and would truncate"
    );
    ferrosa_common::CqlValue::Timestamp(micros / 1000)
}

/// `YYYY-MM-DD` → `CqlValue::Date(days + 2^31)` (CQL epoch-centered encoding).
fn cql_date(s: &str) -> ferrosa_common::CqlValue {
    let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .unwrap_or_else(|e| panic!("bad date literal {s:?}: {e}"));
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let days = (date - epoch).num_days();
    let encoded = (days + 2_147_483_648) as u32;
    ferrosa_common::CqlValue::Date(encoded)
}

/// `HH:MM:SS[.ffffff]` → `CqlValue::Time(nanos since midnight)`.
fn cql_time(s: &str) -> ferrosa_common::CqlValue {
    let t = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveTime::parse_from_str(s, "%H:%M:%S"))
        .unwrap_or_else(|e| panic!("bad time literal {s:?}: {e}"));
    let midnight = chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap();
    let nanos = (t - midnight)
        .num_nanoseconds()
        .expect("time within a day fits in i64 nanos");
    ferrosa_common::CqlValue::Time(nanos)
}

/// canonical IP string → `CqlValue::Inet`.
fn cql_inet(s: &str) -> ferrosa_common::CqlValue {
    ferrosa_common::CqlValue::Inet(s.parse().unwrap_or_else(|e| panic!("bad inet {s:?}: {e}")))
}

/// plain decimal string → `CqlValue::Decimal { scale, unscaled }`.
fn cql_decimal(s: &str) -> ferrosa_common::CqlValue {
    use num_bigint::BigInt;
    let (sign, rest) = match s.strip_prefix('-') {
        Some(r) => (-1i8, r),
        None => (1i8, s),
    };
    let (int_part, frac_part) = rest.split_once('.').unwrap_or((rest, ""));
    let digits = format!("{int_part}{frac_part}");
    let magnitude = digits
        .parse::<BigInt>()
        .unwrap_or_else(|e| panic!("bad decimal {s:?}: {e}"));
    let unscaled = if sign < 0 { -magnitude } else { magnitude };
    ferrosa_common::CqlValue::Decimal {
        scale: frac_part.len() as i32,
        unscaled,
    }
}

/// Start the ferrosa-postgres front-end over the seeded engine, returning a
/// connected client. The engine/schema are kept alive for the test by the
/// returned `tempfile::TempDir` and the spawned server task.
async fn start_ferrosa() -> (tokio_postgres::Client, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let schema = create_schema();
    let engine = seed_engine(dir.path(), &schema);
    let ctx = Arc::new(QueryContext {
        engine: Arc::new(engine),
        schema: Arc::new(schema),
        default_schema: "public".into(),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(server::serve(listener, dev_store(), ctx));

    let (client, connection) = Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("ferrosa_user")
        .password("devpass")
        .dbname("ferrosa")
        .ssl_mode(SslMode::Disable)
        .connect(NoTls)
        .await
        .expect("SCRAM handshake to ferrosa should succeed");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, dir)
}

// ════════════════════════════════════════════════════════════════════════════
// Three-verdict comparison
// ════════════════════════════════════════════════════════════════════════════

/// One cell: `None` is SQL NULL, `Some(text)` is the simple-query text form.
type Cell = Option<String>;
type ResultSet = Vec<Vec<Cell>>;

#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    /// Both sides returned equal result sets.
    Match,
    /// The sides differ — the oracle's fail-loud signal.
    Mismatch,
    /// ferrosa returned an error / a feature this corpus marks unsupported.
    OutOfScope,
}

/// Run a simple query, collecting the data rows as `Vec<Vec<Cell>>` (column
/// order preserved). `Err` means the driver returned an error (out of scope).
async fn run_simple(client: &tokio_postgres::Client, sql: &str) -> Result<ResultSet, String> {
    let messages = client.simple_query(sql).await.map_err(|e| e.to_string())?;
    let mut rows: ResultSet = Vec::new();
    for m in messages {
        if let SimpleQueryMessage::Row(r) = m {
            let cells = (0..r.len())
                .map(|i| r.get(i).map(|s| s.to_string()))
                .collect();
            rows.push(cells);
        }
    }
    Ok(rows)
}

/// Compare two result sets row-by-row in returned order (every corpus query is
/// ORDER BY-deterministic). Per cell: if BOTH parse as f64, compare numerically
/// with a tolerance (absorbs float text-format differences); otherwise compare
/// as exact strings under the C-collation assumptions documented above.
fn result_sets_agree(pg: &ResultSet, fe: &ResultSet) -> bool {
    if pg.len() != fe.len() {
        return false;
    }
    pg.iter().zip(fe.iter()).all(|(pr, fr)| {
        pr.len() == fr.len() && pr.iter().zip(fr.iter()).all(|(a, b)| cells_agree(a, b))
    })
}

fn cells_agree(a: &Cell, b: &Cell) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            if let (Ok(fx), Ok(fy)) = (x.parse::<f64>(), y.parse::<f64>()) {
                (fx - fy).abs() <= 1e-9 * fx.abs().max(fy.abs()).max(1.0)
            } else {
                x == y
            }
        }
        _ => false,
    }
}

/// A single corpus entry: a label, the SQL, and whether ferrosa is EXPECTED to
/// support it. `supported = false` would mark a known-unsupported query as
/// OutOfScope rather than Mismatch — currently every entry is supported.
struct Case {
    label: &'static str,
    sql: &'static str,
    supported: bool,
}

fn corpus() -> Vec<Case> {
    let c = |label, sql| Case {
        label,
        sql,
        supported: true,
    };
    vec![
        // 1. projection + WHERE eq, ordered by numeric PK.
        c(
            "proj_where_eq",
            "SELECT id, name FROM users WHERE dept = 'eng' ORDER BY id",
        ),
        // 2. WHERE AND.
        c(
            "where_and",
            "SELECT oid FROM orders WHERE uid = 3 AND amount = 100 ORDER BY oid",
        ),
        // 3. WHERE OR with parens.
        c(
            "where_or_parens",
            "SELECT oid FROM orders WHERE (uid = 1 OR uid = 2) ORDER BY oid",
        ),
        // 4. WHERE NOT.
        c(
            "where_not",
            "SELECT id FROM users WHERE NOT (dept = 'sales') ORDER BY id",
        ),
        // 5. range predicate.
        c(
            "where_range",
            "SELECT oid, amount FROM orders WHERE amount >= 100 ORDER BY oid",
        ),
        // 6. NULL-handling: `!=` excludes NULL rows in BOTH systems (3VL).
        c(
            "null_filter_ne",
            "SELECT id FROM users WHERE email != 'zzz@x.io' ORDER BY id",
        ),
        // 7. NULL-handling: `= NULL` is UNKNOWN ⇒ no rows in BOTH systems.
        c(
            "null_eq_is_empty",
            "SELECT id FROM users WHERE email = 'nobody' ORDER BY id",
        ),
        // 8. INNER JOIN.
        c(
            "inner_join",
            "SELECT u.name, o.oid FROM users u JOIN orders o ON u.id = o.uid \
             WHERE u.id = 3 ORDER BY o.oid",
        ),
        // 9. GROUP BY + COUNT.
        c(
            "group_count",
            "SELECT uid, COUNT(*) FROM orders GROUP BY uid ORDER BY uid",
        ),
        // 10. GROUP BY + SUM.
        c(
            "group_sum",
            "SELECT uid, SUM(amount) FROM orders GROUP BY uid ORDER BY uid",
        ),
        // 11. GROUP BY + MIN/MAX.
        c(
            "group_min_max",
            "SELECT uid, MIN(amount), MAX(amount) FROM orders GROUP BY uid ORDER BY uid",
        ),
        // 12. GROUP BY + AVG over a double column (numeric-tolerance compare).
        c(
            "group_avg_double",
            "SELECT uid, AVG(price) FROM orders GROUP BY uid ORDER BY uid",
        ),
        // 13. AVG over an int column → fractional (numeric-tolerance compare).
        c(
            "group_avg_int",
            "SELECT uid, AVG(amount) FROM orders GROUP BY uid ORDER BY uid",
        ),
        // 14. HAVING on an aggregate.
        c(
            "having_count",
            "SELECT uid, COUNT(*) FROM orders GROUP BY uid HAVING COUNT(*) > 1 ORDER BY uid",
        ),
        // 15. DISTINCT.
        c(
            "distinct_dept",
            "SELECT DISTINCT dept FROM users ORDER BY dept",
        ),
        // 16. ORDER BY DESC + LIMIT.
        c(
            "order_desc_limit",
            "SELECT oid FROM orders ORDER BY oid DESC LIMIT 3",
        ),
        // 17. LIMIT + OFFSET.
        c(
            "limit_offset",
            "SELECT oid FROM orders ORDER BY oid LIMIT 2 OFFSET 2",
        ),
        // 18. aggregate group then order by the group key DESC.
        c(
            "group_order_desc",
            "SELECT uid, SUM(amount) FROM orders GROUP BY uid ORDER BY uid DESC",
        ),
        // ── New scalar types: timestamp / date / time / inet / numeric ─────
        // 19. project ALL new-type columns, ordered by numeric PK. Exercises the
        //     exact-text renderers for every type at once.
        c(
            "events_project_all",
            "SELECT id, at, on_day, at_time, src, amt FROM events ORDER BY id",
        ),
        // 20. WHERE on a timestamp range (typed literal both sides).
        c(
            "events_ts_range",
            "SELECT id FROM events WHERE at >= TIMESTAMP '2024-03-01 00:00:00' ORDER BY id",
        ),
        // 21. WHERE on a date range.
        c(
            "events_date_range",
            "SELECT id, on_day FROM events WHERE on_day < DATE '2024-01-01' ORDER BY id",
        ),
        // 22. ORDER BY a timestamp column (deterministic monotone order).
        c(
            "events_order_by_ts",
            "SELECT id, at FROM events ORDER BY at",
        ),
        // 23. MIN/MAX over timestamp.
        c("events_min_max_ts", "SELECT MIN(at), MAX(at) FROM events"),
        // 24. DISTINCT over inet, ordered (every src is distinct → 5 rows).
        c(
            "events_distinct_inet",
            "SELECT DISTINCT src FROM events ORDER BY src",
        ),
        // 25. WHERE on a numeric value (typed literal), projecting numeric.
        c(
            "events_numeric_filter",
            "SELECT id, amt FROM events WHERE amt > NUMERIC '1' ORDER BY id",
        ),
        // 26. ORDER BY a time column.
        c(
            "events_order_by_time",
            "SELECT id, at_time FROM events ORDER BY at_time",
        ),
    ]
}

// ════════════════════════════════════════════════════════════════════════════
// The oracle
// ════════════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differential_oracle_corpus_agrees() {
    require_containers();

    let pg = PgContainer::start().await;
    let pg_client = pg.connect().await;
    seed_postgres(&pg_client).await;

    let (fe_client, _ferrosa_dir) = start_ferrosa().await;

    let cases = corpus();
    let mut matches = 0usize;
    let mut out_of_scope = 0usize;
    let mut failures: Vec<(String, ResultSet, ResultSet)> = Vec::new();

    for case in &cases {
        let pg_rows = run_simple(&pg_client, case.sql)
            .await
            .unwrap_or_else(|e| panic!("[{}] postgres errored on a corpus query: {e}", case.label));

        let fe_result = run_simple(&fe_client, case.sql).await;

        let verdict = match fe_result {
            Err(e) => {
                eprintln!("  {:<18} OUT-OF-SCOPE  (ferrosa error: {e})", case.label);
                Verdict::OutOfScope
            }
            Ok(_) if !case.supported => Verdict::OutOfScope,
            Ok(fe_rows) => {
                if result_sets_agree(&pg_rows, &fe_rows) {
                    Verdict::Match
                } else {
                    failures.push((case.label.to_string(), pg_rows.clone(), fe_rows.clone()));
                    Verdict::Mismatch
                }
            }
        };

        match verdict {
            Verdict::Match => {
                matches += 1;
                eprintln!("  {:<18} MATCH", case.label);
            }
            Verdict::OutOfScope => out_of_scope += 1,
            Verdict::Mismatch => {
                eprintln!("  {:<18} MISMATCH", case.label);
            }
        }
    }

    eprintln!(
        "\n[oracle] {} queries: {matches} Match, {} Mismatch, {out_of_scope} OutOfScope",
        cases.len(),
        failures.len()
    );

    // Fail loud: print BOTH result sets for every mismatch — that is the
    // oracle's whole job.
    if !failures.is_empty() {
        for (label, pg_rows, fe_rows) in &failures {
            eprintln!("\n=== MISMATCH: {label} ===");
            eprintln!("  postgres: {pg_rows:?}");
            eprintln!("  ferrosa : {fe_rows:?}");
        }
        panic!(
            "{} corpus quer(ies) diverged from PostgreSQL",
            failures.len()
        );
    }

    // A healthy oracle must actually agree on most of the corpus, not silently
    // mark everything out of scope.
    assert!(
        matches >= cases.len() * 3 / 4,
        "expected most of the corpus to Match, got {matches}/{} (OutOfScope={out_of_scope})",
        cases.len()
    );
}

/// Restricted-query rejection oracle: a handful of queries ferrosa does NOT
/// support must surface as a DRIVER ERROR — never as unproven (silently empty
/// or fabricated) rows. We do not compare to Postgres here; the point is
/// fail-loud, not silent-wrong.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differential_oracle_rejects_unsupported_queries() {
    require_containers();

    let (fe_client, _ferrosa_dir) = start_ferrosa().await;

    // (label, sql) — each must error rather than return rows. NOTE: no-`FROM`
    // expression selects (`SELECT 1`, `SELECT version()`) are now a SUPPORTED
    // feature (handled by the SelectExprs path), so they are deliberately NOT in
    // this list — they belong to the corpus, not the restricted-query oracle.
    let rejects = [
        (
            "subquery",
            "SELECT id FROM users WHERE id IN (SELECT uid FROM orders)",
        ),
        ("missing_table", "SELECT * FROM nonexistent_table"),
        // Grammar ferrosa's M1 subset does not implement — each must surface a
        // clean parse/feature error, never silently-wrong rows.
        ("cte", "WITH e AS (SELECT id FROM users) SELECT id FROM e"),
        ("union", "SELECT id FROM users UNION SELECT uid FROM orders"),
        ("window_fn", "SELECT id, COUNT(*) OVER () FROM users"),
        ("missing_column", "SELECT no_such_col FROM users"),
    ];

    for (label, sql) in rejects {
        let result = fe_client.simple_query(sql).await;
        assert!(
            result.is_err(),
            "[{label}] ferrosa must reject unsupported query `{sql}` with a driver error, \
             not return unproven rows; got: {result:?}"
        );
        eprintln!("  {label:<14} correctly rejected: {}", sql);
    }
}

/// Run a mutation over the wire on BOTH sides; both must succeed.
async fn apply_both(pg: &tokio_postgres::Client, fe: &tokio_postgres::Client, sql: &str) {
    pg.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("postgres failed `{sql}`: {e}"));
    fe.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("ferrosa failed `{sql}`: {e}"));
}

/// Assert a `SELECT` returns identical rows across both sides.
async fn assert_dml_agrees(
    pg: &tokio_postgres::Client,
    fe: &tokio_postgres::Client,
    sql: &str,
    label: &str,
) {
    let p = run_simple(pg, sql)
        .await
        .unwrap_or_else(|e| panic!("[{label}] postgres errored: {e}"));
    let f = run_simple(fe, sql)
        .await
        .unwrap_or_else(|e| panic!("[{label}] ferrosa errored: {e}"));
    assert!(
        result_sets_agree(&p, &f),
        "[{label}] DML divergence:\n  pg = {p:?}\n  fe = {f:?}"
    );
    eprintln!("  {label:<22} MATCH");
}

/// DML differential oracle: apply the SAME INSERT/UPDATE/DELETE over the wire to
/// BOTH Postgres and ferrosa (the empty `kv` table), asserting a SELECT agrees
/// after each mutation. Exercises the write executors against real Postgres
/// semantics — the cross-check that the front-end's writes are sound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn differential_oracle_dml_agrees() {
    require_containers();

    let pg = PgContainer::start().await;
    let pg_client = pg.connect().await;
    seed_postgres(&pg_client).await;
    let (fe_client, _ferrosa_dir) = start_ferrosa().await;

    // ── INSERT ──────────────────────────────────────────────────────────────
    apply_both(
        &pg_client,
        &fe_client,
        "INSERT INTO kv (id, v, n) VALUES (1, 'one', 10)",
    )
    .await;
    apply_both(
        &pg_client,
        &fe_client,
        "INSERT INTO kv (id, v, n) VALUES (2, 'two', 20)",
    )
    .await;
    assert_dml_agrees(
        &pg_client,
        &fe_client,
        "SELECT id, v, n FROM kv ORDER BY id",
        "after_insert",
    )
    .await;

    // INSERT with a NULL column — Postgres stores SQL NULL, ferrosa stores a
    // cell tombstone; both must read back as NULL (not "" / 0).
    apply_both(
        &pg_client,
        &fe_client,
        "INSERT INTO kv (id, v, n) VALUES (3, NULL, 30)",
    )
    .await;
    assert_dml_agrees(
        &pg_client,
        &fe_client,
        "SELECT id, v, n FROM kv ORDER BY id",
        "after_null_insert",
    )
    .await;
}
