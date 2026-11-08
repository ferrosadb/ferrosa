//! W2 — general `UPDATE`/`DELETE` end to end through the front end: the simple
//! protocol, the extended protocol (Parse/Describe/Bind/Execute), transactions,
//! READ COMMITTED ownership (D16) and serializable read sets.
//!
//! `oban_jobs` here is the census table without its array columns: storing
//! `text[]`/`jsonb[]` is lane W3's (array column storage). The array-valued
//! parts of the claim are covered in `ferrosa-sql/tests/dml_exec.rs`.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ferrosa_schema::{
    AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode, EnvSecretsProvider,
    KeyspaceMetadata, PasswordHasher, PasswordPolicy, RateLimitConfig, ReplicationParams, Schema,
    SchemaConfig, TableMetadata, TableParams, TestAuditSink,
};
use ferrosa_storage::{
    CommitLogConfig, CompactionConfig, StorageEngine, StorageEngineConfig, SyncStrategyConfig,
};
use indexmap::IndexMap;
use uuid::Uuid;

use crate::extended::Session;
use crate::messages::BackendMessage;
use crate::server::txn_atomicity_tests::{ctx_with, superuser};
use crate::server::{describe, execute_portal_to, execute_simple_to, QueryContext};

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

const JOB_COLUMNS: [(&str, &str); 9] = [
    ("id", "bigint"),
    ("state", "text"),
    ("queue", "text"),
    ("worker", "text"),
    ("attempt", "int"),
    ("max_attempts", "int"),
    ("priority", "int"),
    ("attempted_at", "timestamp"),
    ("completed_at", "timestamp"),
];

fn jobs_meta() -> TableMetadata {
    let mut columns = IndexMap::new();
    for (name, ty) in JOB_COLUMNS {
        let kind = if name == "id" {
            ColumnKind::PartitionKey
        } else {
            ColumnKind::Regular
        };
        columns.insert(
            name.to_string(),
            ColumnMetadata {
                name: name.to_string(),
                kind,
                position: 0,
                column_type: ty.to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            },
        );
    }
    TableMetadata {
        keyspace: "public".to_string(),
        name: "oban_jobs".to_string(),
        id: Uuid::new_v4(),
        columns,
        partition_key: vec!["id".to_string()],
        clustering_key: vec![],
        params: TableParams::default(),
        flags: HashSet::new(),
        // Oban's `worker NOT NULL` and `attempt_range` CHECK, as W3's DDL records them.
        extensions: HashMap::from([
            crate::pg_constraint::not_null_extension("worker"),
            crate::pg_constraint::check_extension(
                "attempt_range",
                "attempt between 0 and max_attempts",
            ),
        ]),
        is_system: false,
    }
}

/// A context holding `public.oban_jobs` with six jobs:
///
/// | id | state     | queue   | attempt | max | priority |
/// |----|-----------|---------|---------|-----|----------|
/// | 1  | available | default | 0       | 20  | 0        |
/// | 2  | available | default | 0       | 20  | 1        |
/// | 3  | available | other   | 0       | 20  | 0        |
/// | 4  | available | default | 20      | 20  | 0        |
/// | 5  | completed | default | 1       | 20  | 0        |
/// | 6  | available | default | 0       | 20  | 2        |
async fn jobs_ctx() -> (tempfile::TempDir, QueryContext) {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
    let schema = Schema::new(schema_config()).expect("schema bootstraps");
    let auth = superuser();
    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: "public".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options: HashMap::from([("replication_factor".into(), "1".into())]),
                },
            },
            &auth,
        )
        .expect("keyspace");
    let meta = jobs_meta();
    engine.register_table(meta.to_storage_schema()).unwrap();
    schema.create_table(meta, &auth).expect("table");
    let ctx = ctx_with(Arc::new(engine), Arc::new(schema));
    let mut s = Session::new(superuser());
    for (id, state, queue, attempt, prio) in [
        (1, "available", "default", 0, 0),
        (2, "available", "default", 0, 1),
        (3, "available", "other", 0, 0),
        (4, "available", "default", 20, 0),
        (5, "completed", "default", 1, 0),
        (6, "available", "default", 0, 2),
    ] {
        ok(&simple(
            &ctx,
            &mut s,
            &format!(
                "INSERT INTO oban_jobs (id, state, queue, worker, attempt, max_attempts, priority) \
                 VALUES ({id}, '{state}', '{queue}', 'W', {attempt}, 20, {prio})"
            ),
        )
        .await);
    }
    (dir, ctx)
}

async fn simple(ctx: &QueryContext, session: &mut Session, sql: &str) -> Vec<BackendMessage> {
    let mut messages: Vec<BackendMessage> = Vec::new();
    let tail = execute_simple_to(ctx, session, sql, &mut messages)
        .await
        .expect("an in-memory sink cannot fail");
    messages.extend(tail);
    messages
}

fn ok(messages: &[BackendMessage]) {
    assert!(
        !messages
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
        "unexpected error: {messages:?}"
    );
}

fn sqlstate(messages: &[BackendMessage]) -> Option<String> {
    messages.iter().find_map(|m| match m {
        BackendMessage::ErrorResponse { fields } => fields
            .iter()
            .find(|(code, _)| *code == b'C')
            .map(|(_, v)| v.clone()),
        _ => None,
    })
}

fn tag(messages: &[BackendMessage]) -> Option<String> {
    messages.iter().rev().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    })
}

fn rows(messages: &[BackendMessage]) -> Vec<Vec<Option<String>>> {
    messages
        .iter()
        .filter_map(|m| match m {
            BackendMessage::DataRow { columns } => Some(
                columns
                    .iter()
                    .map(|c| c.as_ref().map(|b| String::from_utf8_lossy(b).into_owned()))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

fn text_rows(messages: &[BackendMessage]) -> Vec<Vec<String>> {
    rows(messages)
        .into_iter()
        .map(|r| {
            r.into_iter()
                .map(|c| c.unwrap_or_else(|| "NULL".into()))
                .collect()
        })
        .collect()
}

/// `SELECT id, <cols> FROM oban_jobs ORDER BY id`, as text.
async fn jobs(ctx: &QueryContext, cols: &str) -> Vec<Vec<String>> {
    let mut s = Session::new(superuser());
    let reply = simple(
        ctx,
        &mut s,
        &format!("SELECT id, {cols} FROM oban_jobs ORDER BY id"),
    )
    .await;
    ok(&reply);
    text_rows(&reply)
}

const CLAIM: &str = "WITH subset AS (SELECT so0.id AS id FROM public.oban_jobs AS so0 \
     WHERE (so0.state = 'available') AND (so0.queue = 'default') \
     ORDER BY so0.priority, so0.id LIMIT 3 FOR UPDATE SKIP LOCKED) \
     UPDATE public.oban_jobs AS o0 SET state = 'executing', attempt = o0.attempt + 1 \
     FROM subset AS f1 WHERE (o0.id = f1.id) AND (o0.attempt < o0.max_attempts) \
     RETURNING o0.id, o0.state, o0.attempt";

#[tokio::test]
async fn the_claim_updates_the_subset_and_returns_the_new_rows() {
    let (_dir, ctx) = jobs_ctx().await;
    let mut s = Session::new(superuser());
    let reply = simple(&ctx, &mut s, CLAIM).await;
    ok(&reply);
    assert!(matches!(
        reply.first(),
        Some(BackendMessage::RowDescription { .. })
    ));
    let mut got = text_rows(&reply);
    got.sort();
    // The subset is 1, 4, 2 by (priority, id); 4 is at max_attempts.
    assert_eq!(got, [["1", "executing", "1"], ["2", "executing", "1"]]);
    assert_eq!(tag(&reply).as_deref(), Some("UPDATE 2"));
    assert_eq!(
        jobs(&ctx, "state, attempt").await,
        [
            ["1", "executing", "1"],
            ["2", "executing", "1"],
            ["3", "available", "0"],
            ["4", "available", "20"],
            ["5", "completed", "1"],
            ["6", "available", "0"],
        ]
    );
    // A second claim takes the next job and leaves the claimed ones alone.
    let reply = simple(&ctx, &mut s, CLAIM).await;
    ok(&reply);
    assert_eq!(text_rows(&reply), [["6", "executing", "1"]]);
}

#[tokio::test]
async fn the_ack_matches_on_id_state_any_and_a_timestamp() {
    let (_dir, ctx) = jobs_ctx().await;
    let mut s = Session::new(superuser());
    ok(&simple(&ctx, &mut s, "UPDATE oban_jobs AS o SET attempted_at = TIMESTAMP '2024-01-02 03:04:05' WHERE o.id IN (1, 2)").await);
    let ack = "UPDATE public.oban_jobs AS o0 SET state = 'completed', completed_at = TIMESTAMP '2024-01-02 03:05:00' \
               WHERE (o0.id = 1) AND (o0.state = ANY(ARRAY['executing', 'available'])) \
               AND (o0.attempted_at = TIMESTAMP '2024-01-02 03:04:05')";
    let reply = simple(&ctx, &mut s, ack).await;
    ok(&reply);
    assert_eq!(tag(&reply).as_deref(), Some("UPDATE 1"));
    assert!(rows(&reply).is_empty(), "no RETURNING, no rows");
    assert_eq!(
        jobs(&ctx, "state, completed_at").await[0],
        ["1", "completed", "2024-01-02 03:05:00"]
    );
    // The same ack again matches nothing: the state is no longer in the list.
    let reply = simple(&ctx, &mut s, ack).await;
    ok(&reply);
    assert_eq!(tag(&reply).as_deref(), Some("UPDATE 0"));
}

#[tokio::test]
async fn plugin_delete_and_update_by_in_subquery_with_limit() {
    let (_dir, ctx) = jobs_ctx().await;
    let mut s = Session::new(superuser());
    let reply = simple(
        &ctx,
        &mut s,
        "DELETE FROM public.oban_jobs WHERE id IN \
         (SELECT id FROM public.oban_jobs WHERE state = 'available' AND queue = 'default' ORDER BY id LIMIT 2)",
    )
    .await;
    ok(&reply);
    assert_eq!(tag(&reply).as_deref(), Some("DELETE 2"));
    let ids: Vec<String> = jobs(&ctx, "state")
        .await
        .into_iter()
        .map(|r| r[0].clone())
        .collect();
    assert_eq!(ids, ["3", "4", "5", "6"]);
    let reply = simple(
        &ctx,
        &mut s,
        "UPDATE public.oban_jobs SET state = 'discarded' WHERE id IN \
         (SELECT id FROM public.oban_jobs WHERE attempt >= max_attempts LIMIT 10)",
    )
    .await;
    ok(&reply);
    assert_eq!(tag(&reply).as_deref(), Some("UPDATE 1"));
    assert_eq!(jobs(&ctx, "state").await[1], ["4", "discarded"]);
}

#[tokio::test]
async fn returning_on_a_keyed_update_and_a_delete() {
    let (_dir, ctx) = jobs_ctx().await;
    let mut s = Session::new(superuser());
    let reply = simple(
        &ctx,
        &mut s,
        "UPDATE oban_jobs SET attempt = attempt + 5 WHERE id = 1 RETURNING id, attempt, attempt * 2 AS twice",
    )
    .await;
    ok(&reply);
    assert_eq!(text_rows(&reply), [["1", "5", "10"]]);
    let reply = simple(
        &ctx,
        &mut s,
        "DELETE FROM oban_jobs WHERE id = 5 RETURNING *",
    )
    .await;
    ok(&reply);
    assert_eq!(rows(&reply).len(), 1);
    assert_eq!(rows(&reply)[0].len(), JOB_COLUMNS.len());
    assert_eq!(tag(&reply).as_deref(), Some("DELETE 1"));
}

#[tokio::test]
async fn a_failing_row_writes_nothing() {
    let (_dir, ctx) = jobs_ctx().await;
    let mut s = Session::new(superuser());
    // Division by zero at id = 2, after id = 1 has been written to the statement.
    let reply = simple(
        &ctx,
        &mut s,
        "UPDATE oban_jobs AS o SET attempt = 10 / (o.id - 2)",
    )
    .await;
    assert_eq!(sqlstate(&reply).as_deref(), Some("22012"), "{reply:?}");
    assert!(
        tag(&reply).is_none(),
        "a failed statement has no command tag"
    );
    let attempts: Vec<String> = jobs(&ctx, "attempt")
        .await
        .into_iter()
        .map(|r| r[1].clone())
        .collect();
    assert_eq!(attempts, ["0", "0", "0", "20", "1", "0"]);
}

#[tokio::test]
async fn assigning_the_key_is_refused() {
    let (_dir, ctx) = jobs_ctx().await;
    let mut s = Session::new(superuser());
    let reply = simple(
        &ctx,
        &mut s,
        "UPDATE oban_jobs AS o SET id = 99 WHERE o.state = 'completed'",
    )
    .await;
    assert_eq!(sqlstate(&reply).as_deref(), Some("0A000"), "{reply:?}");
}

#[tokio::test]
async fn a_transaction_sees_its_own_general_update_and_rollback_discards_it() {
    let (_dir, ctx) = jobs_ctx().await;
    let mut s = Session::new(superuser());
    ok(&simple(&ctx, &mut s, "BEGIN").await);
    ok(&simple(
        &ctx,
        &mut s,
        "UPDATE oban_jobs AS o SET state = 'x' WHERE o.queue = 'other'",
    )
    .await);
    // A second general statement in the same transaction reads the first's write.
    let reply = simple(
        &ctx,
        &mut s,
        "UPDATE oban_jobs AS o SET attempt = 7 WHERE o.state = 'x' RETURNING o.id",
    )
    .await;
    ok(&reply);
    assert_eq!(text_rows(&reply), [["3"]]);
    ok(&simple(&ctx, &mut s, "ROLLBACK").await);
    assert_eq!(
        jobs(&ctx, "state, attempt").await[2],
        ["3", "available", "0"]
    );
}

/// D16: under READ COMMITTED the second claimer of a row waits for the first to
/// commit, then finds the row changed and fails with 40001: never a double claim.
#[tokio::test]
async fn read_committed_concurrent_claim_of_one_row_fails_the_second_with_40001() {
    let (_dir, ctx) = jobs_ctx().await;
    let (mut a, mut b) = (Session::new(superuser()), Session::new(superuser()));
    let claim = "UPDATE oban_jobs AS o SET state = 'executing', attempt = o.attempt + 1 \
                 WHERE o.id IN (SELECT id FROM oban_jobs WHERE state = 'available' AND id = 1) RETURNING o.id";
    ok(&simple(&ctx, &mut a, "BEGIN").await);
    let first = simple(&ctx, &mut a, claim).await;
    ok(&first);
    assert_eq!(text_rows(&first), [["1"]]);
    ok(&simple(&ctx, &mut b, "BEGIN").await);
    let (second, commit) = tokio::join!(simple(&ctx, &mut b, claim), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        simple(&ctx, &mut a, "COMMIT").await
    });
    ok(&commit);
    assert_eq!(sqlstate(&second).as_deref(), Some("40001"), "{second:?}");
    ok(&simple(&ctx, &mut b, "ROLLBACK").await);
    assert_eq!(
        jobs(&ctx, "state, attempt").await[0],
        ["1", "executing", "1"]
    );
}

/// A serializable transaction's general UPDATE reads the table; a concurrent
/// commit to it fails the transaction at COMMIT.
#[tokio::test]
async fn a_serializable_general_update_records_its_read_set() {
    let (_dir, ctx) = jobs_ctx().await;
    let (mut t1, mut t2) = (Session::new(superuser()), Session::new(superuser()));
    ok(&simple(&ctx, &mut t1, "BEGIN ISOLATION LEVEL SERIALIZABLE").await);
    ok(&simple(
        &ctx,
        &mut t1,
        "UPDATE oban_jobs AS o SET attempt = 9 WHERE o.state = 'completed'",
    )
    .await);
    ok(&simple(
        &ctx,
        &mut t2,
        "UPDATE oban_jobs SET priority = 3 WHERE id = 6",
    )
    .await);
    let commit = simple(&ctx, &mut t1, "COMMIT").await;
    assert_eq!(sqlstate(&commit).as_deref(), Some("40001"), "{commit:?}");
    assert_eq!(jobs(&ctx, "attempt").await[4], ["5", "1"]);
}

#[tokio::test]
async fn the_extended_protocol_describes_binds_and_executes_a_general_update() {
    let (_dir, ctx) = jobs_ctx().await;
    let mut s = Session::new(superuser());
    let sql = "UPDATE public.oban_jobs AS o0 SET state = $1, attempted_at = $2, attempt = o0.attempt + $3 \
               WHERE (o0.id = $4) AND (o0.state = $5) RETURNING o0.id, o0.attempt, o0.attempted_at";
    assert!(matches!(
        s.on_parse("ack".into(), sql, vec![0; 5]),
        BackendMessage::ParseComplete
    ));
    let described = describe(&ctx, &mut s, b'S', "ack").await;
    let Some(BackendMessage::ParameterDescription { type_oids }) = described.first() else {
        panic!("expected ParameterDescription: {described:?}")
    };
    assert_eq!(type_oids, &[25, 1114, 23, 20, 25]);
    assert!(
        matches!(described.get(1), Some(BackendMessage::RowDescription { fields }) if fields.len() == 3)
    );
    let params: Vec<Option<Vec<u8>>> = ["executing", "2024-01-02 03:04:05", "2", "1", "available"]
        .iter()
        .map(|p| Some(p.as_bytes().to_vec()))
        .collect();
    let bound = s.on_bind(
        String::new(),
        "ack".into(),
        &[],
        &params,
        vec![],
        &crate::jsonb_wire::test_limits(),
    );
    assert!(matches!(bound, BackendMessage::BindComplete), "{bound:?}");
    let mut out: Vec<BackendMessage> = Vec::new();
    let tail = execute_portal_to(&ctx, &mut s, "", 0, &mut out)
        .await
        .unwrap();
    out.extend(tail);
    ok(&out);
    assert!(!out
        .iter()
        .any(|m| matches!(m, BackendMessage::RowDescription { .. })));
    assert_eq!(text_rows(&out), [["1", "2", "2024-01-02 03:04:05"]]);
    assert_eq!(tag(&out).as_deref(), Some("UPDATE 1"));
    assert_eq!(
        jobs(&ctx, "state, attempt").await[0],
        ["1", "executing", "2"]
    );
}

#[tokio::test]
async fn standalone_execution_commits_the_statement_atomically() {
    let (_dir, ctx) = jobs_ctx().await;
    let limits = crate::jsonb_wire::test_limits();
    let reply = crate::query::execute_query(
        &ctx.engine,
        &ctx.schema,
        "UPDATE oban_jobs AS o SET priority = o.priority + 10 WHERE o.queue = 'default' RETURNING o.id",
        "public",
        &limits,
        None,
    )
    .await;
    ok(&reply);
    assert_eq!(tag(&reply).as_deref(), Some("UPDATE 5"));
    // Read back on the same (MVCC-less) path the write took.
    let read = crate::query::execute_query(
        &ctx.engine,
        &ctx.schema,
        "SELECT id, priority FROM oban_jobs ORDER BY id",
        "public",
        &limits,
        None,
    )
    .await;
    ok(&read);
    let prios: Vec<String> = text_rows(&read).into_iter().map(|r| r[1].clone()).collect();
    assert_eq!(prios, ["10", "11", "0", "10", "10", "12"]);
}

/// The complete new row (old row overlaid with the SET values) is checked against
/// NOT NULL and every CHECK, so a cross-column CHECK sees the columns the
/// statement did not assign.
#[tokio::test]
async fn a_general_update_enforces_not_null_and_checks_on_the_new_row() {
    let (_dir, ctx) = jobs_ctx().await;
    let mut s = Session::new(superuser());
    for (sql, state) in [
        (
            "UPDATE oban_jobs AS o SET attempt = 99 WHERE o.id = 1",
            "23514",
        ),
        (
            "UPDATE oban_jobs AS o SET worker = NULL WHERE o.id = 1",
            "23502",
        ),
        (
            "UPDATE oban_jobs AS o SET max_attempts = 0 WHERE o.id = 5",
            "23514",
        ),
    ] {
        let reply = simple(&ctx, &mut s, sql).await;
        assert_eq!(sqlstate(&reply).as_deref(), Some(state), "{sql}: {reply:?}");
    }
    assert_eq!(
        jobs(&ctx, "worker, attempt, max_attempts").await[0],
        ["1", "W", "0", "20"]
    );
    ok(&simple(
        &ctx,
        &mut s,
        "UPDATE oban_jobs AS o SET attempt = 20 WHERE o.id = 1",
    )
    .await);
}

/// `public.jobs2`: Oban's defaulted columns, recorded as W3's DDL records them.
fn defaults_meta() -> TableMetadata {
    let cols = [
        ("id", "bigint"),
        ("state", "text"),
        ("queue", "text"),
        ("worker", "text"),
        ("attempt", "int"),
        ("inserted_at", "timestamp"),
        ("meta", "jsonb"),
    ];
    let mut meta = jobs_meta();
    meta.name = "jobs2".to_string();
    meta.columns
        .retain(|name, _| cols.iter().any(|(n, _)| n == name));
    for (name, ty) in cols {
        meta.columns
            .entry(name.to_string())
            .or_insert_with(|| ColumnMetadata {
                name: name.to_string(),
                kind: ColumnKind::Regular,
                position: 0,
                column_type: ty.to_string(),
                clustering_order: ClusteringOrder::None,
                mask: None,
            });
    }
    meta.extensions = HashMap::from([
        crate::pg_constraint::not_null_extension("worker"),
        crate::pg_constraint::not_null_extension("state"),
        crate::pg_default::serial_extension("id"),
        crate::pg_default::default_extension("state", "'available'"),
        crate::pg_default::default_extension("queue", "'default'"),
        crate::pg_default::default_extension("attempt", "0"),
        crate::pg_default::default_extension("inserted_at", "timezone('UTC', now())"),
        crate::pg_default::default_extension("meta", "'{}'"),
    ]);
    meta
}

async fn defaults_ctx() -> (tempfile::TempDir, QueryContext) {
    let (dir, ctx) = jobs_ctx().await;
    let meta = defaults_meta();
    ctx.engine.register_table(meta.to_storage_schema()).unwrap();
    ctx.schema.create_table(meta, &superuser()).expect("jobs2");
    (dir, ctx)
}

/// INSERT fills every omitted column from its DEFAULT and mints a bigserial id
/// (D15); RETURNING sees the filled values (`Dml.lean`, `fill`).
#[tokio::test]
async fn insert_applies_defaults_and_mints_a_bigserial_id() {
    let (_dir, ctx) = defaults_ctx().await;
    let mut s = Session::new(superuser());
    let before = crate::query::wall_clock_micros();
    let reply = simple(
        &ctx,
        &mut s,
        "INSERT INTO jobs2 (worker) VALUES ('W') RETURNING id, state, queue, attempt, meta",
    )
    .await;
    ok(&reply);
    let got = text_rows(&reply);
    assert_eq!(got.len(), 1);
    let id: i64 = got[0][0].parse().expect("a bigint id");
    assert!(id > 0, "a D15 id is positive: {id}");
    assert_eq!(&got[0][1..], ["available", "default", "0", "{}"]);
    let again = simple(
        &ctx,
        &mut s,
        "INSERT INTO jobs2 (worker) VALUES ('W') RETURNING id",
    )
    .await;
    ok(&again);
    assert_ne!(
        text_rows(&again)[0][0],
        got[0][0],
        "each row gets its own id"
    );
    let read = simple(
        &ctx,
        &mut s,
        &format!("SELECT inserted_at FROM jobs2 WHERE id = {id}"),
    )
    .await;
    ok(&read);
    let stamp = text_rows(&read)[0][0].clone();
    assert_ne!(stamp, "NULL", "inserted_at took timezone('UTC', now())");
    assert!(before > 0 && stamp.starts_with("20"), "{stamp}");
}

#[tokio::test]
async fn an_explicit_value_wins_over_the_default_and_the_serial() {
    let (_dir, ctx) = defaults_ctx().await;
    let mut s = Session::new(superuser());
    let reply = simple(
        &ctx,
        &mut s,
        "INSERT INTO jobs2 (id, worker, state, attempt) VALUES (42, 'W', 'scheduled', 3) RETURNING id, state, attempt, queue",
    )
    .await;
    ok(&reply);
    assert_eq!(text_rows(&reply), [["42", "scheduled", "3", "default"]]);
    // An explicit NULL is a value, not "use the default": NOT NULL refuses it.
    let reply = simple(
        &ctx,
        &mut s,
        "INSERT INTO jobs2 (worker, state) VALUES ('W', NULL)",
    )
    .await;
    assert_eq!(sqlstate(&reply).as_deref(), Some("23502"), "{reply:?}");
}

#[tokio::test]
async fn the_census_insert_over_the_extended_protocol_returns_its_id() {
    let (_dir, ctx) = defaults_ctx().await;
    let mut s = Session::new(superuser());
    let sql = r#"INSERT INTO "public"."jobs2" ("meta","state","queue","worker","attempt") VALUES ($1,$2,$3,$4,$5) RETURNING "id""#;
    assert!(matches!(
        s.on_parse("ins".into(), sql, vec![0; 5]),
        BackendMessage::ParseComplete
    ));
    let described = describe(&ctx, &mut s, b'S', "ins").await;
    assert!(
        matches!(described.get(1), Some(BackendMessage::RowDescription { fields }) if fields.len() == 1),
        "{described:?}"
    );
    let params: Vec<Option<Vec<u8>>> = ["{\"k\": 1}", "available", "default", "Oban.Worker", "0"]
        .iter()
        .map(|p| Some(p.as_bytes().to_vec()))
        .collect();
    let bound = s.on_bind(
        String::new(),
        "ins".into(),
        &[],
        &params,
        vec![],
        &crate::jsonb_wire::test_limits(),
    );
    assert!(matches!(bound, BackendMessage::BindComplete), "{bound:?}");
    let mut out: Vec<BackendMessage> = Vec::new();
    let tail = execute_portal_to(&ctx, &mut s, "", 0, &mut out)
        .await
        .unwrap();
    out.extend(tail);
    ok(&out);
    let id: i64 = text_rows(&out)[0][0].parse().expect("an id");
    assert!(id > 0);
    let read = simple(
        &ctx,
        &mut s,
        &format!("SELECT worker, inserted_at IS NOT NULL FROM jobs2 WHERE id = {id}"),
    )
    .await;
    ok(&read);
    assert_eq!(text_rows(&read), [["Oban.Worker", "t"]]);
}

/// D14: a row-locking clause is accepted and takes no lock; the rows read are
/// the rows a plain SELECT reads.
#[tokio::test]
async fn select_for_update_skip_locked_is_accepted_and_reads_normally() {
    let (_dir, ctx) = jobs_ctx().await;
    let mut s = Session::new(superuser());
    for sql in [
        "SELECT id FROM oban_jobs WHERE state = 'completed' FOR UPDATE SKIP LOCKED",
        "SELECT id FROM oban_jobs WHERE state = 'completed' LIMIT 5 FOR NO KEY UPDATE NOWAIT",
        "SELECT o.id FROM oban_jobs AS o WHERE o.state = 'completed' FOR SHARE OF o",
    ] {
        let reply = simple(&ctx, &mut s, sql).await;
        ok(&reply);
        assert_eq!(text_rows(&reply), [["5"]], "{sql}");
    }
}
