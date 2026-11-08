//! The statements `Oban.insert` and a queue's job execution send (W2 of the Oban
//! programme; `specs/todo/postgres-frontend/oban-compat.md`, census rows 12–15),
//! driven through a real driver (`tokio-postgres`, the extended protocol) against
//! the real server. The schema is built by the server's own DDL from the
//! census's `CREATE TYPE` / `CREATE TABLE`: an enum state, a `bigserial` id and
//! DEFAULTs.
//!
//! The first tests use the table without its array columns; the last,
//! `the_census_dml_runs_verbatim`, builds the census table from the census's own
//! DDL and runs its INSERT, fetch and ack verbatim, arrays included.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use ferrosa_cluster::ddl_path::DdlPath;
use ferrosa_postgres::handshake::VerifierStore;
use ferrosa_postgres::scram::ScramVerifier;
use ferrosa_postgres::{server, AccordAccess, ClusterDdl, QueryContext};
use ferrosa_schema::{
    AuthContext, AuthMethod, DeploymentMode, EnvSecretsProvider, KeyspaceMetadata, PasswordHasher,
    PasswordPolicy, RateLimitConfig, ReplicationParams, Schema, SchemaConfig, TestAuditSink,
};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use tokio::net::TcpListener;
use tokio_postgres::config::SslMode;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};

struct OneRole(ScramVerifier);

impl VerifierStore for OneRole {
    fn verifier(&self, user: &str) -> Option<ScramVerifier> {
        (user == "ferrosa_user").then(|| self.0.clone())
    }
    fn admit(&self, _user: &str) -> Result<(), String> {
        Ok(())
    }
    fn record_failure(&self, _user: &str) {}
    fn record_success(&self, user: &str) -> Result<AuthContext, String> {
        Ok(AuthContext {
            role: user.to_string(),
            is_superuser: true,
            must_change_password: false,
        })
    }
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
        cache_hot_window_secs: 900,
        write_verify: false,
    }
}

struct Fixture {
    client: Client,
    _schema: Arc<Schema>,
    _dir: tempfile::TempDir,
}

async fn connect(port: u16) -> Client {
    let (client, connection) = Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("ferrosa_user")
        .password("devpass")
        .dbname("ferrosa")
        .ssl_mode(SslMode::Disable)
        .connect(NoTls)
        .await
        .expect("SCRAM handshake succeeds");
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("driver connection ended: {error}");
        }
    });
    client
}

fn public_keyspace() -> KeyspaceMetadata {
    KeyspaceMetadata {
        name: "public".to_string(),
        durable_writes: true,
        replication: ReplicationParams {
            strategy: "SimpleStrategy".to_string(),
            options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
        },
    }
}

/// A server whose only user keyspace is `public` (the default schema), with no tables.
async fn start() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).expect("engine"));
    engine
        .register_system_tables()
        .expect("system tables register");
    let schema = Arc::new(Schema::new(schema_config()).expect("schema bootstraps"));
    let auth = AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
    };
    schema
        .create_keyspace(public_keyspace(), &auth)
        .expect("create keyspace");
    let path = Arc::new(ArcSwap::from_pointee(DdlPath::Direct {
        schema: schema.clone(),
        engine: engine.clone(),
    }));
    let ctx = Arc::new(QueryContext {
        cluster_read: Default::default(),
        engine: engine.clone(),
        schema: schema.clone(),
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord: AccordAccess::disabled(),
        ddl: Some(Arc::new(ClusterDdl::new(path))),
        jsonb_limits: ferrosa_postgres::jsonb_wire::test_limits(),
        portals: Default::default(),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let store = Arc::new(OneRole(ScramVerifier::from_password(
        "devpass",
        b"ferrosa-dev-salt",
        4096,
    )));
    tokio::spawn(server::serve(
        listener,
        store,
        ctx,
        server::PgTls::plaintext(),
    ));
    let client = connect(port).await;
    Fixture {
        client,
        _schema: schema,
        _dir: dir,
    }
}

/// The SQLSTATE of a failed statement.
fn code<T: std::fmt::Debug>(result: Result<T, tokio_postgres::Error>) -> String {
    let error = result.expect_err("the statement must fail");
    error
        .code()
        .map(|c| c.code().to_string())
        .unwrap_or_else(|| panic!("error carries no SQLSTATE: {error:?}"))
}

/// The text cells of every row a simple query returns.
async fn rows(client: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("query failed: {sql}: {error:?}"))
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_string))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

/// A jsonb parameter in jsonb's binary format (version byte, then the text).
#[derive(Debug)]
struct Jsonb(&'static str);

impl ToSql for Jsonb {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(&[1]);
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB
    }

    to_sql_checked!();
}

const CREATE_STATE: &str = "CREATE TYPE \"public\".oban_job_state AS ENUM ( 'available', \
     'suspended', 'scheduled', 'executing', 'retryable', 'completed', 'discarded', 'cancelled' )";

/// The census `CREATE TABLE`, with the v12 columns folded in, less the array columns.
const CREATE_JOBS: &str = "CREATE TABLE IF NOT EXISTS \"public\".\"oban_jobs\" (\"id\" bigserial, \
     \"state\" \"public\".oban_job_state DEFAULT 'available' NOT NULL, \
     \"queue\" text DEFAULT 'default' NOT NULL, \"worker\" text NOT NULL, \"args\" jsonb NOT NULL, \
     \"attempt\" integer DEFAULT 0 NOT NULL, \"max_attempts\" integer DEFAULT 20 NOT NULL, \
     \"inserted_at\" timestamp DEFAULT timezone('UTC', now()) NOT NULL, \
     \"scheduled_at\" timestamp DEFAULT timezone('UTC', now()) NOT NULL, \
     \"attempted_at\" timestamp, \"completed_at\" timestamp, \"discarded_at\" timestamp, \
     \"cancelled_at\" timestamp, \"priority\" integer DEFAULT 0 NOT NULL, \
     \"meta\" jsonb DEFAULT '{}', PRIMARY KEY (\"id\"))";

const INSERT: &str = "INSERT INTO \"public\".\"oban_jobs\" (\"args\",\"meta\",\"state\",\"max_attempts\",\"queue\",\"worker\",\"attempt\") VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING \"id\"";

const CLAIM: &str = "WITH \"subset\" AS (SELECT so0.\"id\" AS \"id\" FROM \"public\".\"oban_jobs\" AS so0 WHERE (so0.\"state\" = 'available') AND (so0.\"queue\" = $1) ORDER BY so0.\"priority\", so0.\"scheduled_at\", so0.\"id\" LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE \"public\".\"oban_jobs\" AS o0 SET \"state\" = $3, \"attempted_at\" = $4, \"attempt\" = o0.\"attempt\" + $5 FROM \"subset\" AS f1 WHERE (o0.\"id\" = f1.\"id\") AND (o0.\"attempt\" < o0.\"max_attempts\") RETURNING o0.\"id\", o0.\"state\", o0.\"queue\", o0.\"worker\", o0.\"args\", o0.\"meta\", o0.\"attempt\", o0.\"max_attempts\", o0.\"priority\", o0.\"attempted_at\", o0.\"cancelled_at\", o0.\"completed_at\", o0.\"discarded_at\", o0.\"inserted_at\", o0.\"scheduled_at\"";

const ACK: &str = "UPDATE \"public\".\"oban_jobs\" AS o0 SET \"state\" = $1, \"completed_at\" = $2 WHERE (o0.\"id\" = $3) AND (o0.\"state\" = $4) AND (o0.\"attempted_at\" = $5)";

async fn oban() -> Fixture {
    let fx = start().await;
    fx.client
        .execute(CREATE_STATE, &[])
        .await
        .expect("CREATE TYPE");
    fx.client
        .execute(CREATE_JOBS, &[])
        .await
        .expect("CREATE TABLE oban_jobs");
    fx
}

async fn insert_job(client: &Client, queue: &str) -> i64 {
    let rows = client
        .query(
            INSERT,
            &[
                &Jsonb("{\"n\": 1}"),
                &Jsonb("{}"),
                &"available",
                &20i32,
                &queue,
                &"MyApp.Worker",
                &0i32,
            ],
        )
        .await
        .expect("Oban.insert");
    assert_eq!(rows.len(), 1);
    let id: i64 = rows[0].get(0);
    assert!(id > 0, "a D15 id is a positive bigint: {id}");
    id
}

/// `Oban.insert`, then the queue's fetch, the job's run and its ack: census rows
/// 12, 13 and 14 over the extended protocol.
#[tokio::test]
async fn oban_insert_claim_and_ack_one_job() {
    let fx = oban().await;
    let id = insert_job(&fx.client, "default").await;
    let other = insert_job(&fx.client, "other").await;
    assert_ne!(id, other);
    let defaults = rows(&fx.client, &format!("SELECT state, queue, attempt, priority, inserted_at IS NOT NULL FROM oban_jobs WHERE id = {id}")).await;
    assert_eq!(
        defaults,
        [[
            Some("available".into()),
            Some("default".into()),
            Some("0".into()),
            Some("0".into()),
            Some("t".into())
        ]]
    );

    let at = std::time::SystemTime::now();
    let claimed = fx
        .client
        .query(CLAIM, &[&"default", &5i64, &"executing", &at, &1i32])
        .await
        .expect("the fetch claims");
    assert_eq!(claimed.len(), 1, "only the default-queue job");
    let row = &claimed[0];
    assert_eq!(row.get::<_, i64>(0), id);
    assert_eq!(row.get::<_, &str>(1), "executing");
    assert_eq!(row.get::<_, &str>(3), "MyApp.Worker");
    assert_eq!(
        row.get::<_, i32>(6),
        1,
        "RETURNING sees the incremented attempt"
    );
    let attempted: std::time::SystemTime = row.get(9);

    // A second fetch finds nothing left in the queue.
    let again = fx
        .client
        .query(CLAIM, &[&"default", &5i64, &"executing", &at, &1i32])
        .await
        .expect("an empty fetch");
    assert!(again.is_empty());

    let done = fx
        .client
        .execute(
            ACK,
            &[
                &"completed",
                &std::time::SystemTime::now(),
                &id,
                &"executing",
                &attempted,
            ],
        )
        .await
        .expect("the ack");
    assert_eq!(done, 1);
    let state = rows(
        &fx.client,
        &format!("SELECT state, completed_at IS NOT NULL FROM oban_jobs WHERE id = {id}"),
    )
    .await;
    assert_eq!(state, [[Some("completed".into()), Some("t".into())]]);
    // An ack whose attempted_at no longer matches moves nothing.
    let stale = fx
        .client
        .execute(
            ACK,
            &[
                &"completed",
                &std::time::SystemTime::now(),
                &id,
                &"executing",
                &attempted,
            ],
        )
        .await
        .expect("a stale ack");
    assert_eq!(stale, 0);
}

/// The enum and CHECK-like constraints the DDL recorded apply to a general UPDATE.
#[tokio::test]
async fn a_general_update_validates_the_enum_and_not_null() {
    let fx = oban().await;
    let id = insert_job(&fx.client, "default").await;
    let bad = fx
        .client
        .execute(
            "UPDATE \"public\".\"oban_jobs\" AS o0 SET \"state\" = $1 WHERE o0.\"id\" = $2",
            &[&"bogus", &id],
        )
        .await;
    assert_eq!(code(bad), "22P02");
    let null = fx
        .client
        .execute(
            "UPDATE \"public\".\"oban_jobs\" AS o0 SET \"worker\" = NULL WHERE o0.\"id\" = $1",
            &[&id],
        )
        .await;
    assert_eq!(code(null), "23502");
}

/// The pruner: `DELETE … WHERE id IN (SELECT … LIMIT $n)` (census row 15).
#[tokio::test]
async fn the_pruner_deletes_completed_jobs_in_batches() {
    let fx = oban().await;
    for _ in 0..3 {
        let id = insert_job(&fx.client, "default").await;
        fx.client
            .execute("UPDATE \"public\".\"oban_jobs\" AS o0 SET \"state\" = 'completed' WHERE o0.\"id\" = $1", &[&id])
            .await
            .expect("complete");
    }
    let pruned = fx
        .client
        .execute(
            "DELETE FROM \"public\".\"oban_jobs\" WHERE \"id\" IN (SELECT so0.\"id\" FROM \"public\".\"oban_jobs\" AS so0 WHERE (so0.\"state\" = 'completed') LIMIT $1)",
            &[&2i64],
        )
        .await
        .expect("prune");
    assert_eq!(pruned, 2);
    assert_eq!(rows(&fx.client, "SELECT id FROM oban_jobs").await.len(), 1);
}

/// One census line (1-based), verbatim.
fn census(line: usize) -> &'static str {
    include_str!("fixtures/oban_census.sql")
        .lines()
        .nth(line - 1)
        .expect("a census line")
        .trim_end_matches(';')
}

/// The census's table DDL (lines 10–49, less the PL/pgSQL, trigger, comment and
/// `oban_peers` statements that are W4's or unrelated), then its DML verbatim:
/// `Oban.insert` (54, 58), the fetch (55) and the ack (57), arrays included.
#[tokio::test]
async fn the_census_dml_runs_verbatim() {
    let fx = start().await;
    fx.client
        .execute(CREATE_STATE, &[])
        .await
        .expect("CREATE TYPE");
    for line in [
        10, 11, 12, 13, 17, 18, 19, 20, 22, 24, 25, 26, 27, 28, 29, 30, 31, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 48, 49,
    ] {
        fx.client
            .execute(census(line), &[])
            .await
            .unwrap_or_else(|e| panic!("census line {line}: {e:?}"));
    }
    let no_errors: Vec<Jsonb> = Vec::new();
    let inserted = fx
        .client
        .query(
            census(54),
            &[
                &Jsonb("{\"id\": 1}"),
                &Jsonb("{}"),
                &"available",
                &20i32,
                &"default",
                &"MyApp.Worker",
                &no_errors,
                &vec!["tag-a"],
                &0i32,
            ],
        )
        .await
        .expect("census 54: Oban.insert");
    let id: i64 = inserted[0].get(0);
    let at = std::time::SystemTime::now();
    let claimed = fx
        .client
        .query(
            census(55),
            &[
                &"default",
                &10i64,
                &"executing",
                &at,
                &vec!["node-1"],
                &1i32,
            ],
        )
        .await
        .expect("census 55: the fetch");
    assert_eq!(claimed.len(), 1);
    let job = &claimed[0];
    assert_eq!(job.get::<_, i64>(0), id);
    assert_eq!(job.get::<_, &str>(1), "executing");
    assert_eq!(job.get::<_, Vec<String>>(6), ["tag-a"], "tags");
    assert_eq!(job.get::<_, i32>(8), 1, "attempt");
    assert_eq!(job.get::<_, Vec<String>>(9), ["node-1"], "attempted_by");
    let attempted: std::time::SystemTime = job.get(12);
    let acked = fx
        .client
        .execute(
            census(57),
            &[
                &"completed",
                &std::time::SystemTime::now(),
                &id,
                &vec!["executing", "available"],
                &attempted,
            ],
        )
        .await
        .expect("census 57: the ack");
    assert_eq!(acked, 1);
    let state = rows(
        &fx.client,
        &format!("SELECT state FROM oban_jobs WHERE id = {id}"),
    )
    .await;
    assert_eq!(state, [[Some("completed".to_string())]]);
    let second = fx
        .client
        .query(
            census(58),
            &[
                &Jsonb("{}"),
                &Jsonb("{}"),
                &"available",
                &20i32,
                &"default",
                &"MyApp.Worker",
                &no_errors,
                &0i32,
                &Vec::<&str>::new(),
            ],
        )
        .await
        .expect("census 58: a second Oban.insert");
    assert_ne!(second[0].get::<_, i64>(0), id);
}
