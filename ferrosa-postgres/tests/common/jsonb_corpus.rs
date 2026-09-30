//! Shared jsonb acceptance harness (T-301, D24, D26, D2a, D6b).
//!
//! Included by `tests/jsonb_slice.rs` (no infrastructure, ferrosa only) and by
//! `tests/differential_oracle.rs` (ferrosa against postgres:16) through
//! `#[path]`, so both drive the SAME corpus through the SAME client code. Input
//! never goes through `serde_json::Value`, which drops scale.
//!
//! Each input takes three paths: an untyped literal in a simple query, `$1` in
//! text format, and `$1` in binary format (`0x01` + text). Each stored row is
//! read back in text format (simple query) and in binary format (extended
//! protocol).

// Each including test crate uses a different subset of this module.
#![allow(dead_code)]

use std::collections::HashMap;
use std::error::Error;
use std::path::Path as FsPath;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::BytesMut;
use ferrosa_cluster::ddl_path::DdlPath;
use ferrosa_jsonb::{Limits, LimitsConfig};
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
use tokio_postgres::types::{to_sql_checked, Format, FromSql, IsNull, ToSql, Type};
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};

// ── Wire helpers ────────────────────────────────────────────────────────────

/// A parameter whose bytes and format code the test chooses.
#[derive(Debug)]
pub struct Raw {
    bytes: Vec<u8>,
    text_format: bool,
}

impl Raw {
    pub fn text(s: &str) -> Raw {
        Raw {
            bytes: s.as_bytes().to_vec(),
            text_format: true,
        }
    }

    /// Binary format with exactly these bytes (no version byte added).
    pub fn binary_bytes(bytes: Vec<u8>) -> Raw {
        Raw {
            bytes,
            text_format: false,
        }
    }

    /// Binary jsonb: `version` then the JSON text (`jsonb_send` layout).
    pub fn binary(version: u8, s: &str) -> Raw {
        let mut bytes = vec![version];
        bytes.extend_from_slice(s.as_bytes());
        Raw {
            bytes,
            text_format: false,
        }
    }
}

impl ToSql for Raw {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(&self.bytes);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB || *ty == Type::JSON || *ty == Type::TEXT
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        if self.text_format {
            Format::Text
        } else {
            Format::Binary
        }
    }

    to_sql_checked!();
}

/// A binary-format jsonb result column, bytes untouched.
pub struct RawOut(pub Vec<u8>);

impl<'a> FromSql<'a> for RawOut {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(RawOut(raw.to_vec()))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB
    }
}

/// The SQLSTATE of a database error, or a description of a non-database one.
pub fn code_of(error: &tokio_postgres::Error) -> String {
    error
        .as_db_error()
        .map(|db| db.code().code().to_string())
        .unwrap_or_else(|| format!("not a database error: {error}"))
}

// ── In-process ferrosa with PG DDL ─────────────────────────────────────────

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

fn engine_config(dir: &FsPath) -> StorageEngineConfig {
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

/// A ferrosa PG endpoint with a superuser login and an empty `public`
/// keyspace, so every table is made through PG DDL over the wire. The
/// temporary directory lives as long as the returned value.
pub struct FerrosaPg {
    pub client: Client,
    _dir: tempfile::TempDir,
}

pub async fn start_ferrosa_pg() -> FerrosaPg {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = Arc::new(StorageEngine::new(engine_config(dir.path()), None).expect("engine"));
    let schema = Arc::new(Schema::new(schema_config()).expect("schema bootstraps"));
    let auth = AuthContext {
        role: "cassandra".to_string(),
        is_superuser: true,
        must_change_password: false,
    };
    let mut options = HashMap::new();
    options.insert("replication_factor".to_string(), "1".to_string());
    schema
        .create_keyspace(
            KeyspaceMetadata {
                name: "public".to_string(),
                durable_writes: true,
                replication: ReplicationParams {
                    strategy: "SimpleStrategy".to_string(),
                    options,
                },
            },
            &auth,
        )
        .expect("create keyspace");
    let path = Arc::new(ArcSwap::from_pointee(DdlPath::Direct {
        schema: schema.clone(),
        engine: engine.clone(),
    }));
    let jsonb_limits =
        Limits::from_config(&LimitsConfig::default(), 32 * 1024 * 1024).expect("limits resolve");
    let ctx = Arc::new(QueryContext {
        engine,
        schema,
        default_schema: "public".into(),
        mvcc: Arc::new(ferrosa_postgres::MvccManager::default()),
        accord: AccordAccess::disabled(),
        ddl: Some(Arc::new(ClusterDdl::new(path))),
        jsonb_limits,
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
    FerrosaPg { client, _dir: dir }
}

// ── The corpus ──────────────────────────────────────────────────────────────

/// What Postgres 16 answers for one input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expect {
    /// Accepted; `jsonb` output text is exactly this.
    Text(String),
    /// Refused with this SQLSTATE; no row is left behind.
    Err(&'static str),
}

#[derive(Debug, Clone)]
pub struct Case {
    pub label: String,
    pub input: String,
    pub expect: Expect,
}

fn ok(label: &str, input: &str, out: &str) -> Case {
    Case {
        label: label.to_string(),
        input: input.to_string(),
        expect: Expect::Text(out.to_string()),
    }
}

/// Accepted, and printed as the input was written.
fn same(label: &str, input: &str) -> Case {
    ok(label, input, input)
}

fn bad(label: &str, input: &str, code: &'static str) -> Case {
    Case {
        label: label.to_string(),
        input: input.to_string(),
        expect: Expect::Err(code),
    }
}

fn nest_arrays(depth: usize) -> (String, String) {
    let input = format!("{}1{}", "[".repeat(depth), "]".repeat(depth));
    (input.clone(), input)
}

fn nest_objects(depth: usize) -> (String, String) {
    let input = format!("{}1{}", r#"{"a":"#.repeat(depth), "}".repeat(depth));
    let out = format!("{}1{}", r#"{"a": "#.repeat(depth), "}".repeat(depth));
    (input, out)
}

/// Documents both servers must accept, with Postgres's exact output.
pub fn valid_corpus() -> Vec<Case> {
    let mut v = vec![
        // D2a: scale is preserved, exponents are expanded, -0 prints 0.
        same("scale 1.0", "1.0"),
        same("scale 1.10", "1.10"),
        same("scale 0.00", "0.00"),
        ok("negative zero decimal", "-0.0", "0.0"),
        ok("negative zero integer", "-0", "0"),
        ok("exponent 1e2", "1e2", "100"),
        ok("exponent 1E+2", "1E+2", "100"),
        ok("exponent 1.50e1", "1.50e1", "15.0"),
        ok("exponent -1.5E-3", "-1.5E-3", "-0.0015"),
        ok("exponent 0.1e-5", "0.1e-5", "0.000001"),
        same("beyond i64", "9223372036854775808"),
        same("below i64", "-9223372036854775809"),
        same("beyond u64", "18446744073709551616"),
        same("exact decimal", "12345678901234567890.123"),
        same("exact decimal negative", "-12345678901234567890.123"),
        same(
            "exact decimal scale 1.10 in object",
            r#"[1.10, 2.500, 3.0]"#,
        ),
        // Escapes and non-ASCII: raw characters, and \u00xx in lowercase.
        same("unicode e-acute", r#""é""#),
        ok("escaped e-acute", r#""é""#, r#""é""#),
        ok("surrogate pair", r#""😀""#, r#""😀""#),
        same("raw astral", r#""😀""#),
        same("escaped quote", r#""\"""#),
        same("escaped backslash", r#""\\""#),
        same("named escapes", r#""\b\f\n\r\t""#),
        same("control 0x01", r#""\u0001""#),
        ok("control 0x1F uppercase input", r#""\u001F""#, r#""\u001f""#),
        ok("control 0x0B", r#""\u000b""#, r#""\u000b""#),
        ok("control 0x0e uppercase E", r#""\u000E""#, r#""\u000e""#),
        ok("DEL prints raw", r#""\u007f""#, "\"\u{7f}\""),
        ok("escaped solidus", r#""\/""#, r#""/""#),
        same("empty string", r#""""#),
        same("cjk key", r#"{"日本語": "テスト"}"#),
        // D6b: duplicate keys, last wins.
        ok("duplicate key", r#"{"a":1,"a":2}"#, r#"{"a": 2}"#),
        ok(
            "duplicate key interleaved",
            r#"{"a":1,"b":2,"a":3}"#,
            r#"{"a": 3, "b": 2}"#,
        ),
        ok(
            "duplicate key nested",
            r#"{"o":{"k":1,"k":[1,2]},"o":{"k":"last"}}"#,
            r#"{"o": {"k": "last"}}"#,
        ),
        // Whitespace variants.
        ok(
            "whitespace variants",
            " \t\r\n{  \"a\" :  1 ,\n\"b\":\t[ 1 , 2 ]  }\n ",
            r#"{"a": 1, "b": [1, 2]}"#,
        ),
        // Containers and scalars.
        same("empty object", "{}"),
        same("empty array", "[]"),
        same("array of empty array", "[[]]"),
        same("array of empty object", "[{}]"),
        same("object of empty object", r#"{"a": {}}"#),
        same("null", "null"),
        same("true", "true"),
        same("false", "false"),
        same("zero", "0"),
        same("array of scalars", r#"[null, true, false, 0, "x", []]"#),
        // D26 key order: shortest first, then bytewise.
        ok(
            "key order aa vs b",
            r#"{"aa":2,"b":1}"#,
            r#"{"b": 1, "aa": 2}"#,
        ),
        ok(
            "key order equal length tie-break bytewise",
            r#"{"ab":1,"aa":2,"b":3,"c":4}"#,
            r#"{"b": 3, "c": 4, "aa": 2, "ab": 1}"#,
        ),
        ok(
            "key order length is bytes not chars",
            r#"{"é":1,"z":2,"ab":3}"#,
            r#"{"z": 2, "ab": 3, "é": 1}"#,
        ),
        ok(
            "key order empty key first",
            r#"{"a":2,"":1}"#,
            r#"{"": 1, "a": 2}"#,
        ),
        ok(
            "key order uppercase before lowercase",
            r#"{"b":1,"B":2,"a":3,"A":4}"#,
            r#"{"A": 4, "B": 2, "a": 3, "b": 1}"#,
        ),
        // A realistic document.
        ok(
            "realistic profile",
            concat!(
                r#"{"id":"u-1042","name":"Zoë O'Brien","active":true,"score":97.50,"#,
                r#""balance":12345678901234567890.123,"tags":["a","b\"q\"","日本"],"#,
                r#""address":{"city":"Zürich","geo":{"lat":47.3769,"lon":8.5417},"zip":null},"#,
                r#""history":[{"t":1,"v":1.10},{"t":2,"v":1e2}],"note":"line1\nline2\ttab"}"#
            ),
            concat!(
                r#"{"id": "u-1042", "name": "Zoë O'Brien", "note": "line1\nline2\ttab", "#,
                r#""tags": ["a", "b\"q\"", "日本"], "score": 97.50, "active": true, "#,
                r#""address": {"geo": {"lat": 47.3769, "lon": 8.5417}, "zip": null, "city": "Zürich"}, "#,
                r#""balance": 12345678901234567890.123, "history": [{"t": 1, "v": 1.10}, {"t": 2, "v": 100}]}"#
            ),
        ),
    ];
    let (input, out) = nest_arrays(100);
    v.push(ok("nested arrays depth 100", &input, &out));
    let (input, out) = nest_objects(100);
    v.push(ok("nested objects depth 100", &input, &out));
    // Numbers at the D14a digit caps.
    v.push(same("131072 integer digits", &"9".repeat(131_072)));
    let frac = format!("0.{}", "1".repeat(16_383));
    v.push(same("16383 fraction digits", &frac));
    v.push(ok(
        "exponent 1e400",
        "1e400",
        &format!("1{}", "0".repeat(400)),
    ));
    v.push(ok(
        "exponent 1e-400",
        "1e-400",
        &format!("0.{}1", "0".repeat(399)),
    ));
    v
}

/// Documents both servers must refuse with the same SQLSTATE and leave no row.
pub fn invalid_corpus() -> Vec<Case> {
    vec![
        bad("trailing garbage", r#"{"a":1} x"#, "22P02"),
        bad("NaN", "NaN", "22P02"),
        bad("Infinity", "Infinity", "22P02"),
        bad("lone high surrogate", r#""\ud800""#, "22P02"),
        bad("lone low surrogate", r#""\udc00""#, "22P02"),
        bad("NUL escape", r#""\u0000""#, "22P05"),
        bad("NUL escape in key", r#"{"\u0000":1}"#, "22P05"),
        bad("unbalanced array", "[1,2", "22P02"),
        bad("unbalanced object", r#"{"a":1"#, "22P02"),
        bad("missing value", r#"{"a":}"#, "22P02"),
        bad("single quotes", "{'a':1}", "22P02"),
        bad("trailing comma array", "[1,]", "22P02"),
        bad("trailing comma object", r#"{"a":1,}"#, "22P02"),
        bad("leading zero", "01", "22P02"),
        bad("bare decimal point", "1.", "22P02"),
        bad("leading decimal point", ".5", "22P02"),
        bad("leading plus", "+1", "22P02"),
        bad("empty input", "", "22P02"),
        bad("truncated literal", "tru", "22P02"),
        bad("unterminated string", r#""abc"#, "22P02"),
        bad("bad escape", r#""\q""#, "22P02"),
        bad("raw control character", "\"a\u{1}b\"", "22P02"),
        bad("bare word", "abc", "22P02"),
    ]
}

// ── Running a corpus against one server ─────────────────────────────────────

/// The three ways a jsonb value is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    Literal,
    TextParam,
    BinaryParam,
}

pub const PATHS: [Via; 3] = [Via::Literal, Via::TextParam, Via::BinaryParam];

/// What one server did with one input over one path. The text form is the
/// simple-query result; the binary form is the extended-protocol result bytes
/// (`0x01` + text).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Stored { text: String, binary: Vec<u8> },
    Refused(String),
}

pub const CREATE_TABLE: &str = "CREATE TABLE t (id int PRIMARY KEY, doc jsonb)";

/// Which server: Postgres prints through `doc::text`, ferrosa prints `doc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Server {
    Postgres,
    Ferrosa,
}

impl Server {
    fn text_select(self, id: i32) -> String {
        match self {
            Server::Postgres => format!("SELECT doc::text FROM t WHERE id = {id}"),
            Server::Ferrosa => format!("SELECT doc FROM t WHERE id = {id}"),
        }
    }
}

async fn insert(
    client: &Client,
    path: Via,
    id: i32,
    input: &str,
) -> Result<(), tokio_postgres::Error> {
    match path {
        Via::Literal => {
            let quoted = input.replace('\'', "''");
            client
                .batch_execute(&format!(
                    "INSERT INTO t (id, doc) VALUES ({id}, '{quoted}')"
                ))
                .await
        }
        Via::TextParam | Via::BinaryParam => {
            let param = if path == Via::TextParam {
                Raw::text(input)
            } else {
                Raw::binary(1, input)
            };
            let stmt = client
                .prepare("INSERT INTO t (id, doc) VALUES ($1, $2)")
                .await?;
            client.execute(&stmt, &[&id, &param]).await.map(|_| ())
        }
    }
}

async fn read_text(client: &Client, server: Server, id: i32) -> Result<Option<String>, String> {
    let messages = client
        .simple_query(&server.text_select(id))
        .await
        .map_err(|e| format!("text read failed: {e}"))?;
    let mut found = None;
    for message in messages {
        if let SimpleQueryMessage::Row(row) = message {
            let cell = row.get(0).ok_or("a stored jsonb cell is never NULL")?;
            found = Some(cell.to_string());
        }
    }
    Ok(found)
}

async fn read_binary(client: &Client, id: i32) -> Result<Option<Vec<u8>>, String> {
    let rows = client
        .query(&format!("SELECT doc FROM t WHERE id = {id}"), &[])
        .await
        .map_err(|e| format!("binary read failed: {e}"))?;
    match rows.first() {
        None => Ok(None),
        Some(row) => {
            let out: RawOut = row.try_get(0).map_err(|e| format!("binary decode: {e}"))?;
            Ok(Some(out.0))
        }
    }
}

/// Send one input over one path and read it back both ways. A refused write
/// must leave no row; a row left behind is reported as a failure string, not an
/// outcome, so it can never be mistaken for agreement.
pub async fn run_one(
    client: &Client,
    server: Server,
    path: Via,
    id: i32,
    input: &str,
) -> Result<Outcome, String> {
    match insert(client, path, id, input).await {
        Err(error) => {
            let code = code_of(&error);
            if read_text(client, server, id).await?.is_some() {
                return Err(format!("a refused write ({code}) left a row for id {id}"));
            }
            Ok(Outcome::Refused(code))
        }
        Ok(()) => {
            let text = read_text(client, server, id)
                .await?
                .ok_or_else(|| format!("an accepted write left no row for id {id}"))?;
            let binary = read_binary(client, id)
                .await?
                .ok_or_else(|| format!("binary read found no row for id {id}"))?;
            Ok(Outcome::Stored { text, binary })
        }
    }
}

/// Every case over every path against one server, in corpus order. Ids are
/// unique per (case, path) so no row is overwritten.
pub async fn run_corpus(
    client: &Client,
    server: Server,
    cases: &[Case],
) -> Result<Vec<(String, Via, Outcome)>, String> {
    let mut results = Vec::with_capacity(cases.len() * PATHS.len());
    for (case_no, case) in cases.iter().enumerate() {
        for (path_no, path) in PATHS.iter().enumerate() {
            let id = i32::try_from(case_no * PATHS.len() + path_no)
                .map_err(|_| "corpus too large for an int id".to_string())?;
            let outcome = run_one(client, server, *path, id, &case.input).await?;
            results.push((case.label.clone(), *path, outcome));
        }
    }
    Ok(results)
}

/// SQLSTATE for a binary jsonb value whose version byte is not 1. Postgres 16's
/// `jsonb_recv` uses a bare `elog(ERROR)`, so this is `XX000`; the oracle test
/// asserts both servers give it.
pub const BAD_VERSION_SQLSTATE: &str = "XX000";

/// SQLSTATE for an empty binary jsonb value (no version byte): Postgres 16's
/// message-buffer underflow.
pub const MISSING_VERSION_SQLSTATE: &str = "08P01";

/// The exact binary result for a text form: `0x01` then the text.
pub fn binary_of(text: &str) -> Vec<u8> {
    let mut bytes = vec![1u8];
    bytes.extend_from_slice(text.as_bytes());
    bytes
}
