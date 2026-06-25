//! Tokio TCP front-end that drives the sans-IO [`Connection`] over a socket.
//!
//! This is the thin I/O wrapper: read bytes → `Connection::on_bytes` → write
//! bytes through the handshake, then a post-auth **query loop** that frames
//! simple queries and runs them against the relational engine over live storage.
//! All protocol logic lives in the sans-IO layers (`connection`, `codec`,
//! `query`), so this module stays small and is exercised end-to-end by a real
//! driver in the integration tests.

use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
use bytes::BytesMut;
use ferrosa_schema::Schema;
use ferrosa_storage::StorageEngine;
use rand::RngCore;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::codec::{self, CodecError};
use crate::connection::{ConnError, Connection};
use crate::extended::{self, PreparedKind, Session};
use crate::handshake::{HandshakeError, VerifierStore};
use crate::messages::{BackendMessage, FrontendMessage};
use crate::query;

/// Shared context for the post-auth query phase: the storage engine and schema
/// to resolve and scan tables, plus the default schema (Postgres `search_path`
/// head) bare table names resolve under.
pub struct QueryContext {
    pub engine: Arc<StorageEngine>,
    pub schema: Arc<Schema>,
    pub default_schema: String,
    /// The Accord committer that drives a buffered `BEGIN`/`COMMIT` write-set to
    /// an atomic multi-key transaction. `None` in standalone mode — a `COMMIT`
    /// with buffered DML then fails loud (cluster mode required) rather than
    /// faking atomicity (FMEA PG-1).
    pub accord_committer: Option<Arc<dyn ferrosa_storage::accord::TransactionCommitter>>,
}

/// An unpredictable, printable SCRAM server nonce (base64, so no comma — the one
/// character RFC 5802 forbids in a nonce).
fn random_server_nonce() -> String {
    let mut bytes = [0u8; 18];
    rand::thread_rng().fill_bytes(&mut bytes);
    STANDARD_NO_PAD.encode(bytes)
}

/// SQLSTATE to report for a fatal connection error (fail loud to the client).
fn sqlstate(err: &ConnError) -> &'static str {
    match err {
        ConnError::Handshake(HandshakeError::Scram(_))
        | ConnError::Handshake(HandshakeError::UnknownRole) => "28P01", // invalid_password
        ConnError::Handshake(_) => "28000", // invalid_authorization
        ConnError::Codec(_) | ConnError::Unexpected(_) => "08P01", // protocol_violation
    }
}

/// Encode and write one fatal `ErrorResponse`, then return.
async fn write_fatal<St>(stream: &mut St, code: &str, message: &str) -> std::io::Result<()>
where
    St: AsyncWrite + Unpin,
{
    let mut eb = BytesMut::new();
    BackendMessage::ErrorResponse {
        fields: vec![
            (b'S', "FATAL".to_string()),
            (b'C', code.to_string()),
            (b'M', message.to_string()),
        ],
    }
    .encode(&mut eb);
    stream.write_all(&eb).await
}

/// Drive one connection to completion over `stream`: the SCRAM handshake to
/// `ReadyForQuery`, then the post-auth query loop. Returns on clean close, EOF,
/// or after sending a fatal `ErrorResponse`.
pub async fn handle_connection<St, S>(
    mut stream: St,
    store: Arc<S>,
    ctx: Arc<QueryContext>,
) -> std::io::Result<()>
where
    St: AsyncRead + AsyncWrite + Unpin,
    S: VerifierStore,
{
    let mut conn = Connection::new(&*store, random_server_nonce());
    let mut buf = [0u8; 8192];

    // ── Phase 1: handshake (startup + SCRAM) until ReadyForQuery ──────────────
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(()); // client closed before authenticating
        }
        match conn.on_bytes(&buf[..n]) {
            Ok(out) => {
                if !out.is_empty() {
                    stream.write_all(&out).await?;
                }
                if conn.is_closed() {
                    return Ok(());
                }
                if conn.is_ready() {
                    break;
                }
            }
            Err(e) => {
                let _ = write_fatal(&mut stream, sqlstate(&e), &format!("{e:?}")).await;
                return Ok(());
            }
        }
    }

    // ── Phase 2: query loop ───────────────────────────────────────────────────
    // Seed the frame buffer with any bytes the client pipelined in the same
    // segment as the SASL final response (so a pipelined first query is not lost).
    let mut frames = conn.take_inbuf();
    query_loop(&mut stream, &mut frames, &ctx, &mut buf).await
}

/// Frame and serve queries — both the **simple** (`Q`) and **extended**
/// (`Parse`/`Bind`/`Describe`/`Execute`/`Sync`/`Close`) protocols — until the
/// client terminates or the socket closes. `frames` is seeded with any
/// already-buffered post-auth bytes. A per-connection [`Session`] holds the
/// prepared statements and portals; an error mid-extended-sequence sets a skip
/// flag so subsequent messages are ignored until `Sync`.
async fn query_loop<St>(
    stream: &mut St,
    frames: &mut BytesMut,
    ctx: &QueryContext,
    read_buf: &mut [u8],
) -> std::io::Result<()>
where
    St: AsyncRead + AsyncWrite + Unpin,
{
    let mut session = Session::new();
    loop {
        match codec::read_frontend(frames) {
            Ok(Some(msg)) => {
                if handle_frontend(stream, ctx, &mut session, msg).await? {
                    return Ok(()); // Terminate
                }
            }
            Ok(None) => {
                // Need more bytes for a complete frame.
                let n = stream.read(read_buf).await?;
                if n == 0 {
                    return Ok(()); // EOF: client closed
                }
                frames.extend_from_slice(&read_buf[..n]);
            }
            Err(e) => {
                // Fail loud on a protocol violation, then close.
                let _ = write_fatal(stream, codec_sqlstate(&e), &e.to_string()).await;
                return Ok(());
            }
        }
    }
}

/// Handle one framed frontend message. Returns `Ok(true)` when the client asked
/// to terminate. Encodes any backend replies and writes them to `stream`.
///
/// Extended-protocol error skipping: while `session.is_error_pending()`, every
/// message except `Sync` is ignored (Postgres semantics — the backend discards
/// messages until the next Sync after an error).
async fn handle_frontend<St>(
    stream: &mut St,
    ctx: &QueryContext,
    session: &mut Session,
    msg: FrontendMessage,
) -> std::io::Result<bool>
where
    St: AsyncRead + AsyncWrite + Unpin,
{
    // While an error is pending, skip everything until Sync (which re-readies).
    if session.is_error_pending() && !matches!(msg, FrontendMessage::Sync) {
        return Ok(false);
    }

    let mut out = BytesMut::new();
    match msg {
        FrontendMessage::Query(sql) => {
            let msgs = execute_simple(ctx, session, &sql).await;
            for m in &msgs {
                m.encode(&mut out);
            }
            BackendMessage::ReadyForQuery(session.txn_status()).encode(&mut out);
        }
        FrontendMessage::Parse {
            stmt_name,
            query,
            param_types,
        } => {
            session
                .on_parse(stmt_name, &query, param_types)
                .encode(&mut out);
        }
        FrontendMessage::Bind {
            portal,
            stmt_name,
            param_formats,
            param_values,
            result_formats,
        } => {
            session
                .on_bind(
                    portal,
                    stmt_name,
                    &param_formats,
                    &param_values,
                    result_formats,
                )
                .encode(&mut out);
        }
        FrontendMessage::Describe { kind, name } => {
            for m in describe(ctx, session, kind, &name).await {
                m.encode(&mut out);
            }
        }
        FrontendMessage::Execute { portal, .. } => {
            for m in execute_portal(ctx, session, &portal).await {
                m.encode(&mut out);
            }
        }
        FrontendMessage::Close { kind, name } => {
            session.on_close(kind, &name).encode(&mut out);
        }
        FrontendMessage::Sync => {
            session.on_sync();
            BackendMessage::ReadyForQuery(session.txn_status()).encode(&mut out);
        }
        FrontendMessage::Terminate => return Ok(true),
        // SASL after auth, or any other unexpected message: ignore.
        FrontendMessage::SaslResponse { .. } | FrontendMessage::Unknown { .. } => {}
    }

    if !out.is_empty() {
        stream.write_all(&out).await?;
    }
    Ok(false)
}

/// Execute one simple-query string with transaction-state awareness.
///
/// `BEGIN`/`COMMIT`/`ROLLBACK` drive the session's protocol transaction state
/// (reported in the following `ReadyForQuery`). The front-end is read-only
/// today, so a `COMMIT` has an empty write-set and nothing to apply; once DML
/// lands, the accumulated writes commit here via Accord (blueprint D11 / the
/// `T`-block-triggers-Accord seam). All other statements delegate to the
/// stateless executor; an error inside a transaction aborts it (`T` → `E`), and
/// while aborted only `COMMIT`/`ROLLBACK` are accepted (PG `25P02`).
async fn execute_simple(
    ctx: &QueryContext,
    session: &mut Session,
    sql: &str,
) -> Vec<BackendMessage> {
    let stmt = match ferrosa_sql::parse_statement(sql) {
        Ok(s) => s,
        Err(e) => {
            session.mark_txn_failed();
            return vec![query::error_response("42601", &e.to_string())];
        }
    };

    if session.in_failed_txn()
        && !matches!(
            stmt,
            ferrosa_sql::Statement::Commit | ferrosa_sql::Statement::Rollback
        )
    {
        return vec![query::error_response(
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        )];
    }

    match stmt {
        ferrosa_sql::Statement::Begin => {
            session.begin_txn();
            vec![BackendMessage::CommandComplete {
                tag: "BEGIN".to_string(),
            }]
        }
        ferrosa_sql::Statement::Commit => commit_txn(ctx, session).await,
        ferrosa_sql::Statement::Rollback => {
            // ROLLBACK discards the buffered write-set — those writes were never
            // applied — and leaves the transaction block.
            session.end_txn();
            vec![BackendMessage::CommandComplete {
                tag: "ROLLBACK".to_string(),
            }]
        }
        // Data + session statements: delegate to the stateless executor. (It
        // re-parses; cheap, and keeps the executor self-contained.) In an open
        // transaction, DML is BUFFERED into the session write-set instead of
        // applied; autocommit (no open txn) applies immediately.
        _ => {
            let msgs = if session.in_txn() {
                query::execute_query(
                    &ctx.engine,
                    &ctx.schema,
                    sql,
                    &ctx.default_schema,
                    Some(session.txn_writes_mut()),
                )
                .await
            } else {
                query::execute_query(&ctx.engine, &ctx.schema, sql, &ctx.default_schema, None).await
            };
            if session.in_txn()
                && msgs
                    .iter()
                    .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
            {
                // A statement that fails inside a transaction POISONS it (`T` →
                // `E`): the next COMMIT is rejected (PG `25P02`) rather than
                // committing a partial buffered write-set.
                session.mark_txn_failed();
            }
            msgs
        }
    }
}

/// Drive the buffered `BEGIN`/`COMMIT` write-set through the Accord committer,
/// then leave the transaction block. FAILS LOUD and never applies a buffered
/// write outside the committer:
///
/// * standalone mode (no committer) → ErrorResponse (cluster mode required);
/// * a clean Accord abort → ErrorResponse (`40001`/`25000`);
/// * a commit that cannot reach a decision → ErrorResponse (`58000`).
///
/// In every case the transaction is ended and the buffer dropped, so the server
/// never acks a transaction it did not commit.
async fn commit_txn(ctx: &QueryContext, session: &mut Session) -> Vec<BackendMessage> {
    use ferrosa_storage::accord::CommitOutcome;

    // A COMMIT on an aborted (poisoned) transaction never commits the partial
    // buffer — Postgres treats it as a ROLLBACK. Drop the buffer and report
    // ROLLBACK rather than committing an incomplete write-set.
    if session.in_failed_txn() {
        session.end_txn();
        return vec![BackendMessage::CommandComplete {
            tag: "ROLLBACK".to_string(),
        }];
    }

    let writes = session.take_txn_writes();

    // An empty write-set (`BEGIN; COMMIT;` with no DML, or only reads) is a
    // no-op that commits cleanly — there is nothing to apply, so no atomicity to
    // honor and no committer required (matches the `TransactionCommitter`
    // contract). This is NOT a fake success: zero writes means zero state change.
    if writes.is_empty() {
        session.end_txn();
        return vec![BackendMessage::CommandComplete {
            tag: "COMMIT".to_string(),
        }];
    }

    let committer = match &ctx.accord_committer {
        Some(c) => c.clone(),
        None => {
            session.end_txn();
            // Buffered writes were never applied; fail loud rather than fake a
            // COMMIT we cannot honor atomically.
            return vec![query::error_response(
                "0A000",
                "multi-statement transactions require cluster mode (Accord)",
            )];
        }
    };

    let outcome = committer.commit(writes).await;
    session.end_txn();
    match outcome {
        Ok(CommitOutcome::Committed) => vec![BackendMessage::CommandComplete {
            tag: "COMMIT".to_string(),
        }],
        Ok(CommitOutcome::Aborted { reason }) => vec![query::error_response(
            "40001",
            &format!("transaction aborted: {reason}"),
        )],
        Err(e) => vec![query::error_response(
            "58000",
            &format!("transaction commit failed: {}", e.reason),
        )],
    }
}

/// Handle `Describe`: for a statement (`S`), reply `ParameterDescription` then a
/// `RowDescription`/`NoData`; for a portal (`P`), reply a `RowDescription`/
/// `NoData` under the portal's result formats. On any error, set the skip flag
/// and reply a single `ErrorResponse`.
async fn describe(
    ctx: &QueryContext,
    session: &mut Session,
    kind: u8,
    name: &str,
) -> Vec<BackendMessage> {
    match kind {
        b'S' => {
            let Some(stmt) = session.statement(name) else {
                return vec![session.fail(query::error_response(
                    "26000",
                    &format!("prepared statement \"{name}\" does not exist"),
                ))];
            };
            let parsed = stmt.parsed.clone();
            let declared = stmt.param_oids.clone();
            match parsed {
                PreparedKind::Select(select) => {
                    // ParameterDescription: a driver (e.g. tokio-postgres) relies
                    // on this to serialize bound parameters. Use the
                    // client-declared OID where given (non-zero), else infer the
                    // parameter's type from the column it is compared against.
                    let param_oids = match infer_param_oids(ctx, session, &select, &declared).await
                    {
                        Ok(oids) => oids,
                        Err(err) => return vec![err],
                    };
                    // Persist the resolved OIDs so a later Bind decodes binary
                    // params against the types the driver was told to serialize.
                    session.set_param_oids(name, param_oids.clone());
                    let mut msgs = vec![extended::parameter_description(&param_oids)];
                    match describe_columns(ctx, session, &select).await {
                        Ok(columns) => msgs.push(extended::describe_statement_rows(&columns)),
                        Err(err) => return vec![err], // describe_columns set the flag
                    }
                    msgs
                }
                // No-FROM expression select: no parameters (rejected at parse),
                // so an empty ParameterDescription + columns from the scalars.
                PreparedKind::Exprs(items) => {
                    match query::execute_scalar_select(&items, &ctx.default_schema) {
                        Ok(result) => vec![
                            extended::parameter_description(&[]),
                            extended::describe_statement_rows(&result.columns),
                        ],
                        Err(err) => vec![session.fail(err)],
                    }
                }
            }
        }
        b'P' => {
            let Some(portal) = session.portal(name) else {
                return vec![session.fail(query::error_response(
                    "34000",
                    &format!("portal \"{name}\" does not exist"),
                ))];
            };
            let stmt_name = portal.stmt_name.clone();
            let result_formats = portal.result_formats.clone();
            let Some(stmt) = session.statement(&stmt_name) else {
                return vec![session.fail(query::error_response(
                    "26000",
                    &format!("prepared statement \"{stmt_name}\" does not exist"),
                ))];
            };
            let parsed = stmt.parsed.clone();
            match parsed {
                PreparedKind::Select(select) => {
                    match describe_columns(ctx, session, &select).await {
                        Ok(columns) => {
                            vec![extended::describe_portal_rows(&columns, &result_formats)]
                        }
                        Err(err) => vec![err],
                    }
                }
                PreparedKind::Exprs(items) => {
                    match query::execute_scalar_select(&items, &ctx.default_schema) {
                        Ok(result) => {
                            vec![extended::describe_portal_rows(
                                &result.columns,
                                &result_formats,
                            )]
                        }
                        Err(err) => vec![session.fail(err)],
                    }
                }
            }
        }
        _ => vec![session.fail(query::error_response(
            "08P01",
            "Describe kind must be 'S' or 'P'",
        ))],
    }
}

/// Resolve the parameter type OIDs to advertise in `ParameterDescription`. For
/// each `$N`, prefer the client-declared OID when it is non-zero; otherwise
/// infer the type from the column the parameter is compared against (loading the
/// referenced tables to type-resolve). On failure, set the skip flag and return
/// the `ErrorResponse`.
async fn infer_param_oids(
    ctx: &QueryContext,
    session: &mut Session,
    stmt: &ferrosa_sql::SelectStmt,
    declared: &[i32],
) -> Result<Vec<i32>, BackendMessage> {
    let catalog =
        match query::load_catalog(&ctx.engine, &ctx.schema, stmt, &ctx.default_schema).await {
            Ok(catalog) => catalog,
            Err(err) => return Err(session.fail(err)),
        };
    let inferred = match ferrosa_sql::infer_param_types(stmt, &catalog, &ctx.default_schema) {
        Ok(types) => types,
        Err(e) => return Err(session.fail(extended::describe_exec_error(&e))),
    };
    Ok(inferred
        .iter()
        .enumerate()
        .map(|(i, ty)| match declared.get(i).copied() {
            Some(oid) if oid != 0 => oid,
            _ => query::column_type_oid(*ty),
        })
        .collect())
}

/// Resolve a statement's output columns: load its tables, then call
/// `ferrosa_sql::describe` (no operators / no params). On failure, set the skip
/// flag and return the `ErrorResponse`.
async fn describe_columns(
    ctx: &QueryContext,
    session: &mut Session,
    stmt: &ferrosa_sql::SelectStmt,
) -> Result<Vec<ferrosa_sql::Column>, BackendMessage> {
    let catalog =
        match query::load_catalog(&ctx.engine, &ctx.schema, stmt, &ctx.default_schema).await {
            Ok(catalog) => catalog,
            Err(err) => return Err(session.fail(err)),
        };
    match ferrosa_sql::describe(stmt, &catalog, &ctx.default_schema) {
        Ok(columns) => Ok(columns),
        Err(e) => Err(session.fail(extended::describe_exec_error(&e))),
    }
}

/// Handle `Execute`: load the portal's tables, run the bound query, and emit the
/// result rows encoded per the portal's result formats + `CommandComplete`. On
/// any error, set the skip flag and reply a single `ErrorResponse`. (Does NOT
/// emit `ReadyForQuery` — that follows `Sync`.)
async fn execute_portal(
    ctx: &QueryContext,
    session: &mut Session,
    portal_name: &str,
) -> Vec<BackendMessage> {
    let Some(portal) = session.portal(portal_name) else {
        return vec![session.fail(query::error_response(
            "34000",
            &format!("portal \"{portal_name}\" does not exist"),
        ))];
    };
    let stmt_name = portal.stmt_name.clone();
    let params = portal.params.clone();
    let result_formats = portal.result_formats.clone();

    let Some(stmt) = session.statement(&stmt_name) else {
        return vec![session.fail(query::error_response(
            "26000",
            &format!("prepared statement \"{stmt_name}\" does not exist"),
        ))];
    };
    let parsed = stmt.parsed.clone();

    match parsed {
        PreparedKind::Select(select) => {
            let catalog =
                match query::load_catalog(&ctx.engine, &ctx.schema, &select, &ctx.default_schema)
                    .await
                {
                    Ok(catalog) => catalog,
                    Err(err) => return vec![session.fail(err)],
                };
            let result = ferrosa_sql::execute(&select, &catalog, &ctx.default_schema, &params);
            let errored = result.is_err();
            let msgs = query::render_execute_result(result, &result_formats);
            // On an execution error, set the skip flag so the rest of the
            // sequence is ignored until Sync (Postgres semantics).
            if errored {
                session.mark_error();
            }
            msgs
        }
        // No-FROM expression select: no tables, no params. Evaluate and render.
        PreparedKind::Exprs(items) => {
            match query::execute_scalar_select(&items, &ctx.default_schema) {
                Ok(result) => query::render_execute_result(Ok(result), &result_formats),
                Err(msg) => vec![session.fail(msg)],
            }
        }
    }
}

/// SQLSTATE for a codec-level framing error (always a protocol violation).
fn codec_sqlstate(_err: &CodecError) -> &'static str {
    "08P01" // protocol_violation
}

/// Accept loop: serve Postgres connections from `listener`, one spawned task per
/// connection, until the listener errors. Each connection shares the auth
/// `store` and the query `ctx` (storage + schema).
pub async fn serve<S>(
    listener: TcpListener,
    store: Arc<S>,
    ctx: Arc<QueryContext>,
) -> std::io::Result<()>
where
    S: VerifierStore + Send + Sync + 'static,
{
    loop {
        let (stream, _peer) = listener.accept().await?;
        let store = Arc::clone(&store);
        let ctx = Arc::clone(&ctx);
        tokio::spawn(async move {
            let _ = handle_connection(stream, store, ctx).await;
        });
    }
}

/// Transaction atomicity (FMEA PG-1): a `BEGIN`/`COMMIT` block buffers its DML
/// and only the Accord committer applies it. ROLLBACK discards the buffer (the
/// writes were never applied); COMMIT with no committer fails loud instead of
/// faking atomicity. Runs a real local `StorageEngine` (temp dir, no S3/Docker).
#[cfg(test)]
mod txn_atomicity_tests {
    use super::*;
    use crate::extended::Session;
    use ferrosa_schema::{
        AuthContext, AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode,
        EnvSecretsProvider, KeyspaceMetadata, PasswordHasher, PasswordPolicy, RateLimitConfig,
        ReplicationParams, SchemaConfig, TableMetadata, TableParams, TestAuditSink,
    };
    use ferrosa_storage::accord::MockTransactionCommitter;
    use ferrosa_storage::{
        CommitLogConfig, CompactionConfig, StorageEngineConfig, SyncStrategyConfig,
    };
    use indexmap::IndexMap;
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::time::Duration;
    use uuid::Uuid;

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

    fn schema_with_kv() -> Schema {
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
        let mut cols = IndexMap::new();
        cols.insert(
            "k".to_string(),
            column("k", ColumnKind::PartitionKey, "text"),
        );
        cols.insert("v".to_string(), column("v", ColumnKind::Regular, "text"));
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "kv".to_string(),
                    id: Uuid::new_v4(),
                    columns: cols,
                    partition_key: vec!["k".to_string()],
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

    fn kv_storage_schema() -> ferrosa_common::schema::TableSchema {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        TableSchema {
            keyspace: "public".to_string(),
            table: "kv".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "v".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    fn ctx_with(engine: Arc<StorageEngine>, schema: Arc<Schema>, committer: bool) -> QueryContext {
        QueryContext {
            engine,
            schema,
            default_schema: "public".to_string(),
            accord_committer: if committer {
                Some(Arc::new(MockTransactionCommitter::new()))
            } else {
                None
            },
        }
    }

    async fn make_ctx(committer: bool) -> (tempfile::TempDir, QueryContext) {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
        engine.register_table(kv_storage_schema()).unwrap();
        let ctx = ctx_with(Arc::new(engine), Arc::new(schema_with_kv()), committer);
        (dir, ctx)
    }

    /// Rows visible for key `k`, read back through the `execute_query` SELECT
    /// path with no transaction buffer.
    async fn row_count(ctx: &QueryContext, key: &str) -> usize {
        let msgs = query::execute_query(
            &ctx.engine,
            &ctx.schema,
            &format!("SELECT k FROM kv WHERE k = '{key}'"),
            &ctx.default_schema,
            None,
        )
        .await;
        assert!(
            !msgs
                .iter()
                .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
            "read-back SELECT failed: {msgs:?}"
        );
        msgs.iter()
            .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
            .count()
    }

    fn is_error(msgs: &[BackendMessage], code: &str) -> bool {
        msgs.iter().any(|m| {
            matches!(m, BackendMessage::ErrorResponse { fields }
                if fields.iter().any(|f| *f == (b'C', code.to_string())))
        })
    }

    #[tokio::test]
    async fn rollback_discards_buffered_writes() {
        // BEGIN; INSERT (buffered); ROLLBACK ⇒ the row was never applied.
        // Contrast: an autocommit INSERT IS applied.
        let (_dir, ctx) = make_ctx(true).await;
        let mut session = Session::new();

        let m = execute_simple(&ctx, &mut session, "BEGIN").await;
        assert!(matches!(&m[..], [BackendMessage::CommandComplete { tag }] if tag == "BEGIN"));
        assert!(session.in_txn());

        let m = execute_simple(
            &ctx,
            &mut session,
            "INSERT INTO kv (k, v) VALUES ('r', 'rolledback')",
        )
        .await;
        assert!(
            matches!(&m[..], [BackendMessage::CommandComplete { tag }] if tag == "INSERT 0 1"),
            "buffered INSERT still acks: {m:?}"
        );
        assert_eq!(
            row_count(&ctx, "r").await,
            0,
            "a write buffered inside a txn is NOT yet in storage"
        );

        let m = execute_simple(&ctx, &mut session, "ROLLBACK").await;
        assert!(matches!(&m[..], [BackendMessage::CommandComplete { tag }] if tag == "ROLLBACK"));
        assert!(!session.in_txn());
        assert_eq!(
            row_count(&ctx, "r").await,
            0,
            "ROLLBACK discards the buffer — the write is NEVER applied (FMEA PG-1)"
        );

        // Autocommit contrast: this DML applies immediately.
        let m = execute_simple(
            &ctx,
            &mut session,
            "INSERT INTO kv (k, v) VALUES ('a', 'auto')",
        )
        .await;
        assert!(matches!(&m[..], [BackendMessage::CommandComplete { tag }] if tag == "INSERT 0 1"));
        assert_eq!(
            row_count(&ctx, "a").await,
            1,
            "an autocommit INSERT IS applied immediately"
        );

        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn commit_without_committer_fails_loud() {
        // No committer (standalone mode): BEGIN; INSERT; COMMIT must FAIL LOUD
        // (cluster mode required) and the buffered row must NOT be in storage.
        let (_dir, ctx) = make_ctx(false).await;
        let mut session = Session::new();

        execute_simple(&ctx, &mut session, "BEGIN").await;
        execute_simple(
            &ctx,
            &mut session,
            "INSERT INTO kv (k, v) VALUES ('x', 'never')",
        )
        .await;
        assert_eq!(row_count(&ctx, "x").await, 0, "still buffered, not applied");

        let m = execute_simple(&ctx, &mut session, "COMMIT").await;
        assert!(
            is_error(&m, "0A000"),
            "COMMIT with no committer must fail loud (0A000), got {m:?}"
        );
        assert!(
            m.iter()
                .any(|msg| matches!(msg, BackendMessage::ErrorResponse { fields }
                if fields.iter().any(|f| f.0 == b'M' && f.1.contains("cluster mode")))),
            "the error must mention cluster mode: {m:?}"
        );
        assert!(
            !session.in_txn(),
            "the transaction is ended even on failure"
        );
        assert_eq!(
            row_count(&ctx, "x").await,
            0,
            "a COMMIT that cannot be honored NEVER applies the buffered write"
        );

        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn commit_with_committer_acks_and_records_write_set() {
        // With a committer, COMMIT drives the buffered write-set through it and
        // acks COMMIT. (The mock records the set; it does not itself apply.)
        let (_dir, ctx) = make_ctx(true).await;
        let mut session = Session::new();

        execute_simple(&ctx, &mut session, "BEGIN").await;
        execute_simple(
            &ctx,
            &mut session,
            "INSERT INTO kv (k, v) VALUES ('y', 'committed')",
        )
        .await;
        let m = execute_simple(&ctx, &mut session, "COMMIT").await;
        assert!(
            matches!(&m[..], [BackendMessage::CommandComplete { tag }] if tag == "COMMIT"),
            "COMMIT acks via the committer: {m:?}"
        );
        assert!(!session.in_txn());

        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn failed_txn_commit_does_not_apply() {
        // A statement that errors inside a txn poisons it; subsequent DML hits
        // 25P02; COMMIT is treated as ROLLBACK and nothing is applied.
        let (_dir, ctx) = make_ctx(true).await;
        let mut session = Session::new();

        execute_simple(&ctx, &mut session, "BEGIN").await;
        // A bad INSERT (missing PK column) errors and poisons the txn.
        let m = execute_simple(&ctx, &mut session, "INSERT INTO kv (v) VALUES ('no-pk')").await;
        assert!(
            m.iter()
                .any(|x| matches!(x, BackendMessage::ErrorResponse { .. })),
            "the bad INSERT fails loud: {m:?}"
        );
        assert!(session.in_failed_txn(), "the txn is now poisoned (T → E)");

        // Further DML is rejected with 25P02 until the block ends.
        let m = execute_simple(
            &ctx,
            &mut session,
            "INSERT INTO kv (k, v) VALUES ('z', 'x')",
        )
        .await;
        assert!(is_error(&m, "25P02"), "aborted txn rejects DML: {m:?}");

        // COMMIT on a poisoned txn behaves like ROLLBACK; nothing applied.
        let m = execute_simple(&ctx, &mut session, "COMMIT").await;
        assert!(
            matches!(&m[..], [BackendMessage::CommandComplete { tag }] if tag == "ROLLBACK"),
            "COMMIT on an aborted txn reports ROLLBACK: {m:?}"
        );
        assert!(!session.in_txn() && !session.in_failed_txn());
        assert_eq!(row_count(&ctx, "z").await, 0);

        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn select_inside_txn_still_works() {
        // A read inside a transaction is served normally (it does not buffer).
        let (_dir, ctx) = make_ctx(true).await;
        let mut session = Session::new();
        execute_simple(&ctx, &mut session, "BEGIN").await;
        let m = execute_simple(&ctx, &mut session, "SELECT 1").await;
        assert!(
            m.iter()
                .any(|x| matches!(x, BackendMessage::DataRow { .. })),
            "SELECT 1 inside a txn returns a row: {m:?}"
        );
        assert!(!is_error(&m, "0A000"));
        ctx.engine.shutdown().unwrap();
    }
}
