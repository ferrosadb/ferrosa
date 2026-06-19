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
use crate::extended::{self, Session};
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
        ferrosa_sql::Statement::Commit => {
            // Accord commit seam: read-only front-end ⇒ empty write-set today.
            session.end_txn();
            vec![BackendMessage::CommandComplete {
                tag: "COMMIT".to_string(),
            }]
        }
        ferrosa_sql::Statement::Rollback => {
            session.end_txn();
            vec![BackendMessage::CommandComplete {
                tag: "ROLLBACK".to_string(),
            }]
        }
        // Data + session statements: delegate to the stateless executor. (It
        // re-parses; cheap, and keeps the executor self-contained.)
        _ => {
            let msgs =
                query::execute_query(&ctx.engine, &ctx.schema, sql, &ctx.default_schema).await;
            if session.in_txn()
                && msgs
                    .iter()
                    .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
            {
                session.mark_txn_failed();
            }
            msgs
        }
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

            // ParameterDescription: a driver (e.g. tokio-postgres) relies on this
            // to serialize bound parameters. Use the client-declared OID where
            // given (non-zero), else infer the parameter's type from the column
            // it is compared against.
            let param_oids = match infer_param_oids(ctx, session, &parsed, &declared).await {
                Ok(oids) => oids,
                Err(err) => return vec![err],
            };
            // Persist the resolved OIDs so a later Bind decodes binary params
            // against the same types the driver was told to serialize with.
            session.set_param_oids(name, param_oids.clone());
            let mut msgs = vec![extended::parameter_description(&param_oids)];
            match describe_columns(ctx, session, &parsed).await {
                Ok(columns) => msgs.push(extended::describe_statement_rows(&columns)),
                Err(err) => return vec![err], // describe_columns already set the flag
            }
            msgs
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
            match describe_columns(ctx, session, &parsed).await {
                Ok(columns) => vec![extended::describe_portal_rows(&columns, &result_formats)],
                Err(err) => vec![err],
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

    let catalog =
        match query::load_catalog(&ctx.engine, &ctx.schema, &parsed, &ctx.default_schema).await {
            Ok(catalog) => catalog,
            Err(err) => return vec![session.fail(err)],
        };

    let result = ferrosa_sql::execute(&parsed, &catalog, &ctx.default_schema, &params);
    let errored = result.is_err();
    let msgs = query::render_execute_result(result, &result_formats);
    // On an execution error, set the skip flag so the rest of the sequence is
    // ignored until Sync (Postgres semantics).
    if errored {
        session.mark_error();
    }
    msgs
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
