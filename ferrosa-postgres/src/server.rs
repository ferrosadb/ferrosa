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

use crate::authz;
use crate::codec::{self, CodecError};
use crate::connection::{ConnError, Connection, TlsPolicy};
use crate::extended::{self, PortalRun, PreparedKind, Session};
use crate::handshake::{HandshakeError, VerifierStore};
use crate::messages::{BackendMessage, FrontendMessage};
use crate::mvcc::{MvccCommitError, MvccManager};
use crate::query::{self, ReplySink};
use crate::result_stream::{PumpEnd, ResultStream};
use crate::AccordAccess;

/// Shared context for the post-auth query phase: the storage engine and schema
/// to resolve and scan tables, plus the default schema (Postgres `search_path`
/// head) bare table names resolve under.
pub struct QueryContext {
    pub engine: Arc<StorageEngine>,
    pub schema: Arc<Schema>,
    pub default_schema: String,
    /// PostgreSQL-owned snapshot and serializability state. CQL transactions
    /// continue to use Accord through the CQL session path.
    pub mvcc: Arc<MvccManager>,
    /// Distributed commit coordination for PostgreSQL transactions. PostgreSQL
    /// owns snapshot/version validation; Accord supplies the cluster commit
    /// order and atomic write apply. CQL's transaction path is unchanged.
    ///
    /// The committer is resolved **per statement** (see
    /// [`AccordAccess::committer`]) rather than captured here, because this
    /// listener is built before the node has formed a cluster.
    pub accord: AccordAccess,
    /// Schema-change path for PostgreSQL DDL (`CREATE TABLE`). `None` means the
    /// front-end has no DDL authority (unit-test contexts): DDL is then refused
    /// with `0A000` rather than reported as done.
    pub ddl: Option<Arc<dyn crate::ddl::DdlExecutor>>,
    /// Tunable jsonb ingest limits (D14b), resolved once at startup by the
    /// binary from `[jsonb]` / env and passed in here. There is no default:
    /// every constructor must supply the resolved value. They gate INSERT and
    /// UPDATE input only; reads use the codec's fixed hard ceilings.
    pub jsonb_limits: ferrosa_jsonb::Limits,
    /// Node-wide accounting and limits for portals suspended by `max_rows`:
    /// per connection, per node, and an idle timeout. Shared by every
    /// connection on this listener.
    pub portals: Arc<crate::SuspendedPortals>,
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
        ConnError::Handshake(_) | ConnError::TlsRequired => "28000", // invalid_authorization
        ConnError::Codec(_) | ConnError::Unexpected(_) => "08P01",   // protocol_violation
    }
}

/// Operator-facing text for a fatal connection error. Refusals name their
/// reason so a client log says why the connection was closed.
fn fatal_message(err: &ConnError) -> String {
    match err {
        ConnError::Handshake(HandshakeError::Throttled(reason)) => {
            format!("login throttled for this role (failed-login limiter): {reason}")
        }
        ConnError::Handshake(HandshakeError::LoginRefused(reason)) => {
            format!("login refused: {reason}")
        }
        ConnError::TlsRequired => "TLS is required: this server refuses unencrypted \
             connections ([postgres] require_tls = true); connect with sslmode=require"
            .to_string(),
        other => format!("{other:?}"),
    }
}

/// TLS for the PostgreSQL listener: an acceptor when a certificate is
/// configured, and whether a client must negotiate TLS before its
/// StartupMessage.
///
/// Built through `ferrosa_net::tls`, the same machinery (and the single crypto
/// provider) the CQL and internode listeners use.
#[derive(Clone)]
pub struct PgTls {
    acceptor: Option<tokio_rustls::TlsAcceptor>,
    require: bool,
}

impl PgTls {
    /// Plaintext listener: `SSLRequest` is declined with `N`.
    pub fn plaintext() -> Self {
        Self {
            acceptor: None,
            require: false,
        }
    }

    /// Build from `[postgres] tls_cert` / `tls_key` / `require_tls`.
    ///
    /// `require` without a certificate, or only one of cert/key, is an error —
    /// never a silent fall back to plaintext.
    pub fn from_pem(
        cert_path: Option<&str>,
        key_path: Option<&str>,
        require: bool,
    ) -> Result<Self, String> {
        let config =
            ferrosa_net::tls::optional_server_config("postgres", cert_path, key_path, require, &[])
                .map_err(|e| e.to_string())?;
        Ok(Self {
            acceptor: config.map(tokio_rustls::TlsAcceptor::from),
            require,
        })
    }

    /// Whether TLS is offered to clients (a certificate is configured).
    pub fn offers_tls(&self) -> bool {
        self.acceptor.is_some()
    }

    /// Whether clients must negotiate TLS.
    pub fn requires_tls(&self) -> bool {
        self.require
    }

    fn policy(&self) -> TlsPolicy {
        TlsPolicy {
            offer: self.acceptor.is_some(),
            require: self.require,
        }
    }
}

/// How the startup phase ended.
enum StartupOutcome {
    /// Authenticated; the session is at `ReadyForQuery`.
    Ready,
    /// `S` was sent; the caller must run the TLS handshake.
    TlsUpgrade,
    /// The client left or a fatal error was already reported to it.
    Closed,
}

/// Upper bound on the server-side TLS handshake, as for CQL.
const TLS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Drive `conn` over `stream` through startup (+ SCRAM) until it is ready,
/// asks for a TLS upgrade, or closes.
async fn drive_startup<St, S>(
    stream: &mut St,
    conn: &mut Connection<'_, S>,
    buf: &mut [u8],
) -> std::io::Result<StartupOutcome>
where
    St: AsyncRead + AsyncWrite + Unpin,
    S: VerifierStore,
{
    loop {
        let n = stream.read(buf).await?;
        if n == 0 {
            return Ok(StartupOutcome::Closed); // client closed before authenticating
        }
        match conn.on_bytes(&buf[..n]) {
            Ok(out) => {
                if !out.is_empty() {
                    stream.write_all(&out).await?;
                }
                if conn.is_closed() {
                    return Ok(StartupOutcome::Closed);
                }
                if conn.tls_upgrade_pending() {
                    return Ok(StartupOutcome::TlsUpgrade);
                }
                if conn.is_ready() {
                    return Ok(StartupOutcome::Ready);
                }
            }
            Err(e) => {
                tracing::info!(error = ?e, "PostgreSQL connection refused during startup");
                if let Err(write_error) =
                    write_fatal(stream, sqlstate(&e), &fatal_message(&e)).await
                {
                    tracing::debug!(%write_error, "could not send PostgreSQL startup error response");
                }
                return Ok(StartupOutcome::Closed);
            }
        }
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
    tls: &PgTls,
) -> std::io::Result<()>
where
    St: AsyncRead + AsyncWrite + Unpin,
    S: VerifierStore,
{
    let mut conn = Connection::new(&*store, random_server_nonce(), tls.policy());
    let mut buf = [0u8; 8192];

    // ── Phase 1: startup (+ optional TLS upgrade) + SCRAM until ReadyForQuery ─
    match drive_startup(&mut stream, &mut conn, &mut buf).await? {
        StartupOutcome::Closed => Ok(()),
        StartupOutcome::Ready => serve_authenticated(stream, conn, &ctx, &mut buf).await,
        StartupOutcome::TlsUpgrade => {
            let Some(acceptor) = &tls.acceptor else {
                // The policy only offers TLS when an acceptor exists, so this
                // is a wiring bug, not a client error.
                return Err(std::io::Error::other(
                    "PostgreSQL connection asked for a TLS upgrade with no acceptor configured",
                ));
            };
            let mut tls_stream =
                match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                    Ok(Ok(tls_stream)) => tls_stream,
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "PostgreSQL TLS handshake failed");
                        return Ok(());
                    }
                    Err(_) => {
                        tracing::warn!("PostgreSQL TLS handshake timed out");
                        return Ok(());
                    }
                };
            if let Err(error) = conn.on_tls_established() {
                return Err(std::io::Error::other(format!(
                    "PostgreSQL TLS state machine out of step: {error:?}"
                )));
            }
            match drive_startup(&mut tls_stream, &mut conn, &mut buf).await? {
                StartupOutcome::Closed => Ok(()),
                StartupOutcome::Ready => {
                    serve_authenticated(tls_stream, conn, &ctx, &mut buf).await
                }
                // The connection refuses a second SSLRequest itself, so a
                // second upgrade request cannot get here.
                StartupOutcome::TlsUpgrade => Err(std::io::Error::other(
                    "PostgreSQL connection asked for a second TLS upgrade",
                )),
            }
        }
    }
}

/// Phase 2: the post-auth query loop, run as the authenticated role.
async fn serve_authenticated<St, S>(
    mut stream: St,
    mut conn: Connection<'_, S>,
    ctx: &QueryContext,
    buf: &mut [u8],
) -> std::io::Result<()>
where
    St: AsyncRead + AsyncWrite + Unpin,
    S: VerifierStore,
{
    let Some(auth) = conn.auth_context().cloned() else {
        // Ready without an authenticated role would run queries unauthorized.
        return Err(std::io::Error::other(
            "PostgreSQL session reached ReadyForQuery without an authenticated role",
        ));
    };
    // Seed the frame buffer with any bytes the client pipelined in the same
    // segment as the SASL final response (so a pipelined first query is not lost).
    let mut frames = conn.take_inbuf();
    query_loop(&mut stream, &mut frames, ctx, auth, buf).await
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
    auth: ferrosa_schema::AuthContext,
    read_buf: &mut [u8],
) -> std::io::Result<()>
where
    St: AsyncRead + AsyncWrite + Unpin,
{
    let mut session = Session::new(auth);
    let idle_timeout = ctx.portals.limits().idle_timeout;
    loop {
        match codec::read_frontend(frames) {
            Ok(Some(msg)) => {
                // `COPY ... FROM STDIN` is the one statement that cannot be answered in a single
                // step: the client sends the payload only AFTER `CopyInResponse`. So it is driven
                // here, where the frame buffer, the stream and the read buffer are in scope,
                // rather than from `handle_frontend`.
                if let FrontendMessage::Query(sql) = &msg {
                    // A leading `COPY` is the ONLY shape that can need this path, and the check is
                    // a byte compare rather than a parse: every ordinary statement must not pay for
                    // a second parse here (`execute_simple_to` parses it once, below).
                    let head = sql.trim_start();
                    if !session.is_error_pending()
                        && head.len() >= 4
                        && head.as_bytes()[..4].eq_ignore_ascii_case(b"copy")
                    {
                        if let Ok(ferrosa_sql::Statement::CopyFromStdin(copy)) =
                            ferrosa_sql::parse_statement(sql)
                        {
                            crate::copy_stdin::drive(
                                stream,
                                frames,
                                read_buf,
                                ctx,
                                &mut session,
                                &copy,
                            )
                            .await?;
                            continue;
                        }
                    }
                }
                if handle_frontend(stream, ctx, &mut session, msg).await? {
                    return Ok(()); // Terminate
                }
            }
            Ok(None) => {
                // Need more bytes for a complete frame. A client that sends
                // nothing must not keep its suspended portals forever, so the
                // wait ends early when the longest-idle one expires.
                let n = match session.next_expiry(idle_timeout) {
                    None => stream.read(read_buf).await?,
                    Some(deadline) => tokio::select! {
                        // `read` is cancel-safe: no bytes are lost if the
                        // deadline wins.
                        read = stream.read(read_buf) => read?,
                        () = tokio::time::sleep_until(deadline.into()) => {
                            let closed =
                                session.expire_idle(std::time::Instant::now(), idle_timeout);
                            ctx.portals.record_expiries(closed);
                            continue;
                        }
                    },
                };
                if n == 0 {
                    return Ok(()); // EOF: client closed
                }
                frames.extend_from_slice(&read_buf[..n]);
            }
            Err(e) => {
                // Fail loud on a protocol violation, then close.
                if let Err(write_error) =
                    write_fatal(stream, codec_sqlstate(&e), &e.to_string()).await
                {
                    tracing::debug!(%write_error, "could not send PostgreSQL protocol error response");
                }
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
            let msgs = execute_simple_to(ctx, session, &sql, &mut SocketSink(&mut *stream)).await?;
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
                    &ctx.jsonb_limits,
                )
                .encode(&mut out);
        }
        FrontendMessage::Describe { kind, name } => {
            for m in describe(ctx, session, kind, &name).await {
                m.encode(&mut out);
            }
        }
        FrontendMessage::Execute { portal, max_rows } => {
            let tail = execute_portal_to(
                ctx,
                session,
                &portal,
                max_rows,
                &mut SocketSink(&mut *stream),
            )
            .await?;
            for m in &tail {
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
        // COPY data with no COPY in progress. The wire layer understands these frames so a COPY
        // can be implemented, but the server does not open one yet — so any of them arriving here
        // means the client and we disagree about what is in flight. That is reported rather than
        // ignored: swallowing the payload would let a client's data be reinterpreted as SQL.
        FrontendMessage::CopyData { .. }
        | FrontendMessage::CopyDone
        | FrontendMessage::CopyFail { .. } => {
            crate::query::error_response("08P01", "COPY data received outside a COPY operation")
                .encode(&mut out);
        }
        // SASL after auth, or any other unexpected message: ignore.
        FrontendMessage::SaslResponse { .. } | FrontendMessage::Unknown { .. } => {}
    }

    if !out.is_empty() {
        stream.write_all(&out).await?;
    }
    Ok(false)
}

async fn begin_implicit_transaction(
    ctx: &QueryContext,
    session: &mut Session,
) -> Result<(), BackendMessage> {
    // Per statement: this node may have become a cluster (or lost its write
    // path) since the listener was built.
    let Some(committer) = ctx.accord.committer() else {
        return Ok(());
    };
    let cluster_ts = committer
        .begin_postgres_snapshot(&ctx.default_schema)
        .await
        .map_err(|error| {
            query::error_response(
                "58000",
                &format!("could not establish PostgreSQL transaction snapshot: {error}"),
            )
        })?;
    session.begin_txn(
        Some(ferrosa_sql::IsolationLevel::Serializable),
        ctx.mvcc.snapshot_with_cluster_ts(cluster_ts),
    );
    Ok(())
}

/// Writes each batch of messages to the socket as it is produced, so a streaming
/// `SELECT` never gathers its rows.
struct SocketSink<'a, St>(&'a mut St);

impl<St: AsyncWrite + Unpin> ReplySink for SocketSink<'_, St> {
    async fn send(&mut self, messages: Vec<BackendMessage>) -> std::io::Result<()> {
        let mut buf = BytesMut::new();
        for message in &messages {
            message.encode(&mut buf);
        }
        self.0.write_all(&buf).await
    }
}

/// Treat standalone PostgreSQL data statements as implicit transactions in
/// cluster mode, so autocommit has the same Accord ordering as explicit BEGIN.
///
/// A `SELECT`'s rows are written to `out` as they are produced; the returned
/// messages are the tail still to send (its `CommandComplete`, or the
/// `ErrorResponse` that ended it).
///
/// `pub(crate)` so the `COPY ... FROM STDIN` tests can drive `BEGIN`/`COMMIT`
/// through the SAME transaction path a connection uses, rather than
/// re-implementing it: a COPY's transactionality is only proven by committing
/// and rolling back through this seam.
pub(crate) async fn execute_simple_to<O: ReplySink>(
    ctx: &QueryContext,
    session: &mut Session,
    sql: &str,
    out: &mut O,
) -> std::io::Result<Vec<BackendMessage>> {
    let is_data_statement = matches!(
        ferrosa_sql::parse_statement(sql),
        Ok(ferrosa_sql::Statement::Select(_)
            | ferrosa_sql::Statement::Insert(_)
            | ferrosa_sql::Statement::Update(_)
            | ferrosa_sql::Statement::Delete(_)
            // TRUNCATE is a replicated WRITE too. It MUST take this path, or an
            // autocommit TRUNCATE would bypass Accord entirely and write to LOCAL
            // storage only — a scope hole worse than the reserved key's RF subset.
            // Wrapping it in an implicit transaction routes the tombstone through
            // the cluster commit (and, there, to every serving node at CL=ALL).
            | ferrosa_sql::Statement::Truncate(_))
    );
    if session.in_txn() || ctx.accord.committer().is_none() || !is_data_statement {
        return execute_simple_inner(ctx, session, sql, out).await;
    }
    if let Err(error) = begin_implicit_transaction(ctx, session).await {
        return Ok(vec![error]);
    }

    let messages = execute_simple_inner(ctx, session, sql, out).await?;
    if messages
        .iter()
        .any(|message| matches!(message, BackendMessage::ErrorResponse { .. }))
    {
        session.end_txn();
        return Ok(messages);
    }

    let commit_messages = commit_txn(ctx, session).await;
    Ok(match commit_messages.first() {
        Some(BackendMessage::CommandComplete { tag }) if tag == "COMMIT" => messages,
        Some(BackendMessage::ErrorResponse { .. }) => commit_messages,
        _ => vec![query::error_response(
            "58000",
            "implicit PostgreSQL transaction did not commit",
        )],
    })
}

/// Execute one simple-query string with transaction-state awareness.
///
/// `BEGIN`/`COMMIT`/`ROLLBACK` drive the session's protocol transaction state
/// (reported in the following `ReadyForQuery`). PostgreSQL DML buffers in the
/// session and commits through the PostgreSQL MVCC manager. All other statements delegate to the
/// stateless executor; an error inside a transaction aborts it (`T` → `E`), and
/// while aborted only `COMMIT`/`ROLLBACK` are accepted (PG `25P02`).
async fn execute_simple_inner<O: ReplySink>(
    ctx: &QueryContext,
    session: &mut Session,
    sql: &str,
    out: &mut O,
) -> std::io::Result<Vec<BackendMessage>> {
    let stmt = match ferrosa_sql::parse_statement(sql) {
        Ok(s) => s,
        Err(e) => {
            session.mark_txn_failed();
            return Ok(vec![query::error_response(
                query::parse_error_sqlstate(&e),
                &e.to_string(),
            )]);
        }
    };

    if session.in_failed_txn()
        && !matches!(
            stmt,
            ferrosa_sql::Statement::Commit | ferrosa_sql::Statement::Rollback
        )
    {
        return Ok(vec![query::error_response(
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        )]);
    }

    // t_e1c819ad: authorize against the role before anything touches storage.
    if let Err(denied) = authz::authorize(
        &ctx.schema,
        session.auth(),
        &authz::statement_permissions(&stmt, &ctx.default_schema),
    ) {
        session.mark_txn_failed();
        return Ok(vec![denied]);
    }

    match stmt {
        ferrosa_sql::Statement::Begin { isolation } => {
            Ok(begin_block(ctx, session, isolation).await)
        }
        ferrosa_sql::Statement::Commit => Ok(commit_txn(ctx, session).await),
        ferrosa_sql::Statement::Rollback => {
            // ROLLBACK discards the buffered write-set — those writes were never
            // applied — and leaves the transaction block.
            session.end_txn();
            Ok(vec![BackendMessage::CommandComplete {
                tag: "ROLLBACK".to_string(),
            }])
        }
        // Data + session statements: delegate to the stateless executor. (It
        // re-parses; cheap, and keeps the executor self-contained.) In an open
        // transaction, DML is BUFFERED into the session write-set instead of
        // applied; autocommit (no open txn) applies immediately.
        other => run_data_statement(ctx, session, sql, &other, out).await,
    }
}

/// `BEGIN`: take the transaction's snapshot and open the block.
async fn begin_block(
    ctx: &QueryContext,
    session: &mut Session,
    isolation: Option<ferrosa_sql::IsolationLevel>,
) -> Vec<BackendMessage> {
    if isolation.is_some_and(|level| level != ferrosa_sql::IsolationLevel::Serializable) {
        return vec![query::error_response(
            "0A000",
            "only SERIALIZABLE isolation is supported for explicit PostgreSQL transactions",
        )];
    }
    let snapshot = if let Some(committer) = ctx.accord.committer() {
        match committer.begin_postgres_snapshot(&ctx.default_schema).await {
            Ok(cluster_ts) => ctx.mvcc.snapshot_with_cluster_ts(cluster_ts),
            Err(error) => {
                return vec![query::error_response(
                    "58000",
                    &format!("could not establish PostgreSQL transaction snapshot: {error}"),
                )];
            }
        }
    } else {
        ctx.mvcc.snapshot()
    };
    session.begin_txn(isolation, snapshot);
    vec![BackendMessage::CommandComplete {
        tag: "BEGIN".to_string(),
    }]
}

/// Record a `SELECT`'s tables in the open serializable transaction's read set,
/// so commit can detect a phantom or write-skew against them.
fn track_select_reads(ctx: &QueryContext, session: &mut Session, select: &ferrosa_sql::SelectStmt) {
    if !(session.in_txn()
        && session.txn_isolation() == Some(ferrosa_sql::IsolationLevel::Serializable))
    {
        return;
    }
    let read_tables = session.txn_read_tables_mut();
    read_tables.insert(format!(
        "{}.{}",
        select.from.schema.as_deref().unwrap_or(&ctx.default_schema),
        select.from.table
    ));
    if let Some(join) = &select.join {
        read_tables.insert(format!(
            "{}.{}",
            join.table.schema.as_deref().unwrap_or(&ctx.default_schema),
            join.table.table
        ));
    }
}

/// The environment a read runs against under `snapshot`.
fn read_env<'a>(
    ctx: &'a QueryContext,
    snapshot: &'a crate::mvcc::MvccSnapshot,
) -> query::ReadEnv<'a> {
    query::ReadEnv {
        engine: &ctx.engine,
        schema: &ctx.schema,
        default_schema: &ctx.default_schema,
        mvcc: Some(&ctx.mvcc),
        snapshot: Some(snapshot),
        ddl: ctx.ddl.as_deref(),
        jsonb_limits: &ctx.jsonb_limits,
    }
}

/// The storage and limits context for one extended-protocol DML statement.
pub(crate) fn dml_context<'a>(
    ctx: &'a QueryContext,
    txn: Option<&'a mut Vec<crate::PgWrite>>,
) -> query::DmlContext<'a> {
    query::DmlContext {
        engine: &ctx.engine,
        mvcc: Some(&ctx.mvcc),
        schema: &ctx.schema,
        default_schema: &ctx.default_schema,
        txn,
        jsonb_limits: &ctx.jsonb_limits,
    }
}

/// The context for a no-`FROM` expression select: storage over the session's
/// current snapshot (so a scalar subquery can run), plus the session's pending
/// writes so the inner query sees the caller's uncommitted rows.
fn scalar_read_ctx<'a>(
    ctx: &'a QueryContext,
    session: &'a Session,
    snapshot: &'a crate::mvcc::MvccSnapshot,
) -> query::ScalarReadCtx<'a> {
    query::ScalarReadCtx::new(read_env(ctx, snapshot), Some(session.txn_writes()))
}

/// The snapshot a read runs at: the transaction's own under serializable
/// isolation, otherwise the current one.
fn read_snapshot(ctx: &QueryContext, session: &Session) -> crate::mvcc::MvccSnapshot {
    if session.in_txn()
        && session.txn_isolation() == Some(ferrosa_sql::IsolationLevel::Serializable)
    {
        session
            .txn_snapshot()
            .cloned()
            .unwrap_or_else(|| ctx.mvcc.snapshot())
    } else {
        ctx.mvcc.snapshot()
    }
}

/// Run a data or session statement from the simple-query path.
async fn run_data_statement<O: ReplySink>(
    ctx: &QueryContext,
    session: &mut Session,
    sql: &str,
    stmt: &ferrosa_sql::Statement,
    out: &mut O,
) -> std::io::Result<Vec<BackendMessage>> {
    if let Some(error) = expired_transaction_error(ctx, session) {
        session.mark_txn_failed();
        return Ok(vec![error]);
    }
    if let ferrosa_sql::Statement::Select(select) = stmt {
        track_select_reads(ctx, session, select);
    }
    let snapshot = read_snapshot(ctx, session);
    let in_txn = session.in_txn();
    let msgs = query::execute_query_streaming(
        read_env(ctx, &snapshot),
        sql,
        if in_txn {
            Some(session.txn_writes_mut())
        } else {
            None
        },
        out,
    )
    .await?;
    if in_txn
        && msgs
            .iter()
            .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
    {
        // A statement that fails inside a transaction POISONS it (`T` →
        // `E`): the next COMMIT is rejected (PG `25P02`) rather than
        // committing a partial buffered write-set.
        session.mark_txn_failed();
    }
    Ok(msgs)
}

fn expired_transaction_error(ctx: &QueryContext, session: &Session) -> Option<BackendMessage> {
    let snapshot = session.txn_snapshot()?;
    match ctx
        .mvcc
        .validate_commit(snapshot, &std::collections::HashSet::new())
    {
        Ok(()) => None,
        Err(MvccCommitError::SnapshotExpired) => Some(query::error_response(
            "40001",
            "PostgreSQL transaction snapshot expired",
        )),
        Err(MvccCommitError::SerializationFailure) => Some(query::error_response(
            "40001",
            "could not serialize PostgreSQL transaction",
        )),
        // Backpressure answers 53000 (retryable); a real fault keeps 58000.
        Err(MvccCommitError::Storage(error)) if error.is_backpressure() => Some(
            query::error_response("53000", &format!("transaction refused: {error}")),
        ),
        Err(MvccCommitError::Storage(error)) => Some(query::error_response(
            "58000",
            &format!("transaction validation failed: {error}"),
        )),
    }
}

/// Validate PostgreSQL snapshot conflicts and commit the buffered write-set.
/// In cluster mode, Accord establishes the distributed order and atomic apply;
/// in standalone mode, the MVCC manager applies the batch locally. CQL
/// transaction behavior remains in its separate session path.
///
/// In every case the transaction is ended and the buffer dropped, so the server
/// never acks a transaction it did not commit.
async fn commit_txn(ctx: &QueryContext, session: &mut Session) -> Vec<BackendMessage> {
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
    let read_tables = session.take_txn_read_tables();
    let snapshot = session
        .txn_snapshot()
        .cloned()
        .unwrap_or_else(|| ctx.mvcc.snapshot());

    // An empty write-set (`BEGIN; COMMIT;` with no DML, or only reads) is a
    // no-op that commits cleanly — there is nothing to apply, so no atomicity to
    // honor. This is NOT a fake success: zero writes means zero state change.
    if writes.is_empty() {
        if let Err(error) = ctx.mvcc.validate_commit(&snapshot, &read_tables) {
            session.end_txn();
            return match error {
                MvccCommitError::SerializationFailure | MvccCommitError::SnapshotExpired => {
                    vec![query::error_response(
                        "40001",
                        "could not serialize PostgreSQL transaction",
                    )]
                }
                MvccCommitError::Storage(error) if error.is_backpressure() => {
                    vec![query::error_response(
                        "53000",
                        &format!("transaction refused: {error}"),
                    )]
                }
                MvccCommitError::Storage(error) => vec![query::error_response(
                    "58000",
                    &format!("transaction commit failed: {error}"),
                )],
            };
        }
        if let Some(committer) = ctx.accord.committer() {
            let Some(cluster_snapshot) = snapshot.cluster_timestamp() else {
                session.end_txn();
                return vec![query::error_response(
                    "58000",
                    "cluster PostgreSQL transaction has no Accord snapshot timestamp",
                )];
            };
            let outcome = committer
                .validate_postgres_snapshot(&ctx.default_schema, cluster_snapshot)
                .await;
            session.end_txn();
            return match outcome {
                Ok(true) => {
                    vec![BackendMessage::CommandComplete {
                        tag: "COMMIT".to_string(),
                    }]
                }
                Ok(false) => {
                    vec![query::error_response(
                        "40001",
                        "could not serialize PostgreSQL transaction",
                    )]
                }
                Err(error) => vec![query::error_response(
                    "58000",
                    &format!("transaction commit failed: {error}"),
                )],
            };
        }
        session.end_txn();
        return vec![BackendMessage::CommandComplete {
            tag: "COMMIT".to_string(),
        }];
    }

    let mut write_tables = read_tables;
    let mutations: Vec<_> = writes
        .into_iter()
        .map(|write| {
            write_tables.insert(format!("{}.{}", write.0.keyspace, write.0.table));
            write.0
        })
        .collect();
    let _commit_guard = ctx.mvcc.commit_guard().await;
    // Per-phase attribution for the COMMIT (see `MvccProfile`). Zero-valued and
    // unused unless `FERROSA_PG_COMMIT_PROFILE` is set.
    let commit_started = std::time::Instant::now();
    let mut prepare_nanos: u64 = 0;
    let mut accord_nanos: u64 = 0;
    let outcome = if let Some(committer) = ctx.accord.committer() {
        if let Err(error) = ctx.mvcc.validate_commit(&snapshot, &write_tables) {
            Err(error)
        } else {
            // One streaming pass builds the Accord write-set: each partition's
            // row-version metadata is produced, encoded and handed off as the
            // mutation is visited, so no whole-table row-image map and no
            // per-partition re-grouping map exist at the COMMIT peak. `mutations`
            // is consumed here; the raw whole-table write-set is therefore gone by
            // the time Accord runs, instead of staying resident alongside
            // `accord_writes` for the whole PreAccept/Commit/Apply sequence.
            let prepare_started = std::time::Instant::now();
            let accord_writes =
                match query::prepare_accord_writes(&ctx.engine, &ctx.schema, mutations) {
                    Ok(writes) => writes,
                    Err(error) => {
                        session.end_txn();
                        return vec![query::error_response(
                            "58000",
                            &format!("transaction commit failed: {error:?}"),
                        )];
                    }
                };
            prepare_nanos = u64::try_from(prepare_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let Some(cluster_snapshot) = snapshot.cluster_timestamp() else {
                session.end_txn();
                return vec![query::error_response(
                    "58000",
                    "cluster PostgreSQL transaction has no Accord snapshot timestamp",
                )];
            };
            let tables = write_tables.iter().cloned().collect();
            let accord_started = std::time::Instant::now();
            let accord_result = match committer
                .commit_postgres(&ctx.default_schema, accord_writes, tables, cluster_snapshot)
                .await
            {
                Ok(ferrosa_storage::accord::CommitOutcome::Committed) => {
                    Ok(ctx.mvcc.current_commit_seq())
                }
                Ok(ferrosa_storage::accord::CommitOutcome::Aborted { .. }) => {
                    Err(MvccCommitError::SerializationFailure)
                }
                // Accord's `CommitError` is itself only a `reason: String`,
                // so the typed error is already gone by the time it
                // reaches here — a THIRD erasure, in the committer trait,
                // too deep to widen in this change. Wrapping the reason in
                // `InvalidData` at least reaches `is_backpressure()`'s
                // documented string branch (`starts_with("overloaded:")`),
                // which matches `Error::Overloaded`'s Display, so a
                // distributed commit refused for pressure can still be
                // classified. Best-effort by construction, not by accident.
                Err(error) => Err(MvccCommitError::Storage(
                    ferrosa_common::Error::InvalidData(error.reason),
                )),
            };
            accord_nanos = u64::try_from(accord_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            accord_result
        }
    } else {
        query::commit_mutations(
            &ctx.engine,
            &ctx.schema,
            &ctx.mvcc,
            &snapshot,
            &write_tables,
            mutations,
        )
    };
    session.end_txn();
    if std::env::var_os("FERROSA_PG_COMMIT_PROFILE").is_some() {
        // Attribution line for a large transaction's commit: how the wall time
        // splits between building the Accord write-set (row decode + encode),
        // driving Accord (registration + apply fan-out), and the rest; and how
        // much resident MVCC history the transaction left behind.
        let stats = ctx.mvcc.history_stats();
        let total_ms = commit_started.elapsed().as_secs_f64() * 1_000.0;
        tracing::info!(
            total_ms,
            prepare_ms = prepare_nanos as f64 / 1_000_000.0,
            accord_ms = accord_nanos as f64 / 1_000_000.0,
            decoded_rows = ctx.mvcc.profile_decoded(),
            versions_inserted = ctx.mvcc.profile_versions_inserted(),
            history_keys = stats.total_keys(),
            history_versions = stats.total_versions(),
            history_kib = stats.total_bytes() / 1024,
            dist_keys = stats.distributed_keys,
            dist_versions = stats.distributed_versions,
            dist_kib = stats.distributed_bytes / 1024,
            applied_accord = stats.applied_accord_txns,
            "pg commit phase attribution"
        );
        for (label, calls, nanos) in ctx.mvcc.profile_report() {
            if calls > 0 {
                tracing::info!(
                    phase = label,
                    calls,
                    total_ms = nanos as f64 / 1_000_000.0,
                    per_call_us = nanos as f64 / 1_000.0 / calls as f64,
                    "pg commit phase attribution: mvcc"
                );
            }
        }
    }
    match outcome {
        Ok(_) => vec![BackendMessage::CommandComplete {
            tag: "COMMIT".to_string(),
        }],
        Err(MvccCommitError::SerializationFailure) => vec![query::error_response(
            "40001",
            "could not serialize PostgreSQL transaction",
        )],
        Err(MvccCommitError::SnapshotExpired) => vec![query::error_response(
            "40001",
            "PostgreSQL transaction snapshot expired",
        )],
        // The final outcome arm, which every non-empty explicit transaction
        // lands on. Backpressure is retryable (53000); a real fault is not
        // (58000). This arm answered 58000 unconditionally, so a transactional
        // client was told its commit hit a system fault when the server was
        // only refusing the pace.
        Err(MvccCommitError::Storage(error)) if error.is_backpressure() => {
            vec![query::error_response(
                "53000",
                &format!("transaction refused: {error}"),
            )]
        }
        // An abandoned transaction — the operator-configured dependency-wait
        // bound expired and the transaction was rolled back — is NOT committed
        // and is safe to retry. Report it as a retryable serialization failure
        // (40001), never as an opaque 58000 fault: the client must be able to
        // tell "retry me" from "something is broken". Classified by the
        // `abandoned:` prefix for the same reason `is_backpressure()` above is.
        Err(MvccCommitError::Storage(error)) if matches!(&error, ferrosa_common::Error::InvalidData(msg) if msg.contains("abandoned:")) =>
        {
            vec![query::error_response(
                "40001",
                &format!("transaction was abandoned and NOT committed: {error}"),
            )]
        }
        Err(e @ MvccCommitError::Storage(_)) => vec![query::error_response(
            "58000",
            &format!("transaction commit failed: {e:?}"),
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
            // Describe resolves the referenced tables' columns; a role that may
            // not run the statement learns nothing about those tables either.
            if let Err(denied) = authz::authorize(
                &ctx.schema,
                session.auth(),
                &authz::prepared_permissions(&parsed, &ctx.default_schema),
            ) {
                return vec![session.fail(denied)];
            }
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
                    let snapshot = read_snapshot(ctx, session);
                    let scalar_ctx = scalar_read_ctx(ctx, session, &snapshot);
                    match query::execute_scalar_select(&items, scalar_ctx).await {
                        Ok(result) => vec![
                            extended::parameter_description(&[]),
                            extended::describe_statement_rows(&result.columns),
                        ],
                        Err(err) => vec![session.fail(err)],
                    }
                }
                // Parameterized DML: advertise one OID per `$N` placeholder so a
                // driver that does NOT pre-declare OIDs (tokio-postgres) learns
                // the count. Each OID is the client-declared type where given,
                // else `0` (unspecified ⇒ lenient Bind decode). We persist these
                // so the subsequent Bind decodes against the same OIDs. The
                // result shape is a RowDescription for INSERT RETURNING, else
                // NoData (UPDATE/DELETE never return rows here).
                PreparedKind::Insert(ins) => {
                    let param_oids = match query::infer_insert_param_oids(
                        &ctx.schema,
                        &ins,
                        &ctx.default_schema,
                        &declared,
                    ) {
                        Ok(oids) => oids,
                        Err(err) => return vec![session.fail(err)],
                    };
                    session.set_param_oids(name, param_oids.clone());
                    match query::describe_insert_returning(&ctx.schema, &ins, &ctx.default_schema) {
                        Ok(Some(columns)) => vec![
                            extended::parameter_description(&param_oids),
                            extended::describe_statement_rows(&columns),
                        ],
                        Ok(None) => vec![
                            extended::parameter_description(&param_oids),
                            BackendMessage::NoData,
                        ],
                        Err(err) => vec![session.fail(err)],
                    }
                }
                PreparedKind::Update(upd) => {
                    let param_oids = match query::infer_update_param_oids(
                        &ctx.schema,
                        &upd,
                        &ctx.default_schema,
                        &declared,
                    ) {
                        Ok(oids) => oids,
                        Err(err) => return vec![session.fail(err)],
                    };
                    session.set_param_oids(name, param_oids.clone());
                    vec![
                        extended::parameter_description(&param_oids),
                        BackendMessage::NoData,
                    ]
                }
                PreparedKind::Delete(del) => {
                    let param_oids = match query::infer_delete_param_oids(
                        &ctx.schema,
                        &del,
                        &ctx.default_schema,
                        &declared,
                    ) {
                        Ok(oids) => oids,
                        Err(err) => return vec![session.fail(err)],
                    };
                    session.set_param_oids(name, param_oids.clone());
                    vec![
                        extended::parameter_description(&param_oids),
                        BackendMessage::NoData,
                    ]
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
            if let Err(denied) = authz::authorize(
                &ctx.schema,
                session.auth(),
                &authz::prepared_permissions(&parsed, &ctx.default_schema),
            ) {
                return vec![session.fail(denied)];
            }
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
                    let snapshot = read_snapshot(ctx, session);
                    let scalar_ctx = scalar_read_ctx(ctx, session, &snapshot);
                    match query::execute_scalar_select(&items, scalar_ctx).await {
                        Ok(result) => {
                            vec![extended::describe_portal_rows(
                                &result.columns,
                                &result_formats,
                            )]
                        }
                        Err(err) => vec![session.fail(err)],
                    }
                }
                // DML portal: a RowDescription for INSERT RETURNING (under the
                // portal's result formats), else NoData. UPDATE/DELETE: NoData.
                PreparedKind::Insert(ins) => {
                    match query::describe_insert_returning(&ctx.schema, &ins, &ctx.default_schema) {
                        Ok(Some(columns)) => {
                            vec![extended::describe_portal_rows(&columns, &result_formats)]
                        }
                        Ok(None) => vec![BackendMessage::NoData],
                        Err(err) => vec![session.fail(err)],
                    }
                }
                PreparedKind::Update(_) | PreparedKind::Delete(_) => {
                    vec![BackendMessage::NoData]
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
    // Type inference reads only the catalog's schemas, never its rows, so the
    // streaming providers opened here never touch storage.
    let (catalog, _scans) =
        match query::load_catalog(&ctx.engine, &ctx.schema, stmt, &ctx.default_schema).await {
            Ok(loaded) => loaded,
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
    // Describe resolves columns from the catalog's schemas alone; no scan runs,
    // so no rows are read to answer it.
    let (catalog, _scans) =
        match query::load_catalog(&ctx.engine, &ctx.schema, stmt, &ctx.default_schema).await {
            Ok(loaded) => loaded,
            Err(err) => return Err(session.fail(err)),
        };
    match ferrosa_sql::describe(stmt, &catalog, &ctx.default_schema) {
        Ok(columns) => Ok(columns),
        Err(e) => Err(session.fail(extended::describe_exec_error(&e))),
    }
}

/// Handle `Execute`: run the bound query and emit its rows encoded per the
/// portal's result formats. A `SELECT` streams its `DataRow`s to `out` as the
/// executor yields them and stops at `max_rows` (0 = no limit) with
/// `PortalSuspended`, keeping the running query on the portal so the next
/// `Execute` continues where this one stopped. The returned messages are the
/// tail still to send (`CommandComplete`, `PortalSuspended`, or the
/// `ErrorResponse` that ended a failed query). On any error, set the skip flag.
/// (Does NOT emit `ReadyForQuery` — that follows `Sync`.)
async fn execute_portal_to<O: ReplySink>(
    ctx: &QueryContext,
    session: &mut Session,
    portal_name: &str,
    max_rows: i32,
    out: &mut O,
) -> std::io::Result<Vec<BackendMessage>> {
    let is_data_statement = session
        .portal(portal_name)
        .and_then(|portal| session.statement(&portal.stmt_name))
        .is_some_and(|stmt| {
            matches!(
                &stmt.parsed,
                PreparedKind::Select(_)
                    | PreparedKind::Exprs(_)
                    | PreparedKind::Insert(_)
                    | PreparedKind::Update(_)
                    | PreparedKind::Delete(_)
            )
        });
    if session.in_txn() || ctx.accord.committer().is_none() || !is_data_statement {
        return execute_portal_body(ctx, session, portal_name, max_rows, out).await;
    }
    if let Err(error) = begin_implicit_transaction(ctx, session).await {
        return Ok(vec![session.fail(error)]);
    }

    let messages = execute_portal_body(ctx, session, portal_name, max_rows, out).await?;
    if messages
        .iter()
        .any(|message| matches!(message, BackendMessage::ErrorResponse { .. }))
    {
        session.end_txn();
        return Ok(messages);
    }

    let commit_messages = commit_txn(ctx, session).await;
    Ok(match commit_messages.first() {
        Some(BackendMessage::CommandComplete { tag }) if tag == "COMMIT" => messages,
        Some(BackendMessage::ErrorResponse { .. }) => {
            session.mark_error();
            commit_messages
        }
        _ => {
            session.mark_error();
            vec![query::error_response(
                "58000",
                "implicit PostgreSQL transaction did not commit",
            )]
        }
    })
}

/// Route a portal to the streaming `SELECT` path or the bounded-reply path.
async fn execute_portal_body<O: ReplySink>(
    ctx: &QueryContext,
    session: &mut Session,
    portal_name: &str,
    max_rows: i32,
    out: &mut O,
) -> std::io::Result<Vec<BackendMessage>> {
    // PostgreSQL answers a portal already run to its end with its completion
    // tag and nothing else. Re-running it would return a query's rows again,
    // or apply a DML statement a second time.
    if let Some(tag) = session.finished_tag(portal_name) {
        return Ok(vec![BackendMessage::CommandComplete {
            tag: tag.to_string(),
        }]);
    }
    let is_select = session
        .portal(portal_name)
        .and_then(|portal| session.statement(&portal.stmt_name))
        .is_some_and(|stmt| matches!(&stmt.parsed, PreparedKind::Select(_)));
    if is_select {
        return execute_select_portal(ctx, session, portal_name, max_rows, out).await;
    }
    let messages = execute_portal_inner(ctx, session, portal_name).await;
    let succeeded = !messages
        .iter()
        .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }));
    let tag = messages.iter().rev().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    });
    if let (true, Some(tag)) = (succeeded, tag) {
        session.finish(portal_name.to_string(), tag);
    }
    Ok(messages)
}

/// The `SELECT` a portal is bound to, with its bound parameters.
fn portal_select(
    session: &Session,
    portal_name: &str,
) -> Option<(Box<ferrosa_sql::SelectStmt>, Vec<ferrosa_sql::Value>)> {
    let portal = session.portal(portal_name)?;
    let stmt = session.statement(&portal.stmt_name)?;
    match &stmt.parsed {
        PreparedKind::Select(select) => Some((select.clone(), portal.params.clone())),
        PreparedKind::Exprs(_)
        | PreparedKind::Insert(_)
        | PreparedKind::Update(_)
        | PreparedKind::Delete(_) => None,
    }
}

/// Start a portal's `SELECT`: check the snapshot, record the read, load the
/// tables and launch the executor.
async fn open_portal_stream(
    ctx: &QueryContext,
    session: &mut Session,
    portal_name: &str,
) -> Result<ResultStream, BackendMessage> {
    let Some((select, params)) = portal_select(session, portal_name) else {
        return Err(query::error_response(
            "34000",
            &format!("portal \"{portal_name}\" does not exist"),
        ));
    };
    if let Some(error) = expired_transaction_error(ctx, session) {
        session.mark_txn_failed();
        return Err(error);
    }
    track_select_reads(ctx, session, &select);
    let snapshot = read_snapshot(ctx, session);
    query::open_select_stream(
        read_env(ctx, &snapshot),
        *select,
        Some(session.txn_writes()),
        params,
    )
    .await
}

/// Execute a portal bound to a `SELECT`, resuming its suspended query if it has
/// one and starting it otherwise.
async fn execute_select_portal<O: ReplySink>(
    ctx: &QueryContext,
    session: &mut Session,
    portal_name: &str,
    max_rows: i32,
    out: &mut O,
) -> std::io::Result<Vec<BackendMessage>> {
    let (mut stream, slot) = match session.take_run(portal_name) {
        Some(PortalRun::Suspended(query)) => (query.stream, Some(query.slot)),
        Some(PortalRun::Finished(tag)) => {
            // Answered in `execute_portal_body`; kept for the exhaustive match.
            session.finish(portal_name.to_string(), tag.clone());
            return Ok(vec![BackendMessage::CommandComplete { tag }]);
        }
        Some(PortalRun::Closed(error)) => {
            session.close_run(portal_name.to_string(), error.clone());
            return Ok(vec![session.fail(error)]);
        }
        None => {
            // A portal that may suspend (`max_rows` set) takes its place
            // under the connection and node limits BEFORE it runs, so a
            // refusal reaches the client before any `DataRow`, as PostgreSQL
            // refuses a resource limit before output. If it completes
            // without suspending, the place is given back.
            let slot = if max_rows > 0 {
                match session.admit_suspension(&ctx.portals) {
                    Ok(slot) => Some(slot),
                    Err(refusal) => {
                        session.close_run(portal_name.to_string(), refusal.clone());
                        return Ok(vec![session.fail(refusal)]);
                    }
                }
            } else {
                None
            };
            match open_portal_stream(ctx, session, portal_name).await {
                Ok(stream) => (stream, slot),
                Err(error) => return Ok(vec![session.fail(error)]),
            }
        }
    };
    let result_formats = session
        .portal(portal_name)
        .map(|portal| portal.result_formats.clone())
        .unwrap_or_default();
    if result_formats.len() > 1 && result_formats.len() != stream.columns().len() {
        return Ok(vec![session.fail(query::error_response(
            "08P01",
            "Bind result format count must be zero, one, or match the result column count",
        ))]);
    }
    let limit = usize::try_from(max_rows).ok().filter(|n| *n > 0);
    let end = stream.pump(limit, &result_formats, out).await?;
    match &end {
        PumpEnd::Suspended => {
            // Only an Execute with `max_rows` suspends, and every such
            // Execute holds its place (taken above, or kept from before).
            let Some(slot) = slot else {
                unreachable!("a portal suspended without max_rows");
            };
            session.park(
                portal_name.to_string(),
                stream,
                slot,
                std::time::Instant::now(),
            );
        }
        // Skip the rest of the sequence until Sync (PostgreSQL semantics).
        PumpEnd::Failed(_) => session.mark_error(),
        // A further Execute returns no rows: "SELECT 0".
        PumpEnd::Complete { .. } => session.finish(portal_name.to_string(), "SELECT 0".into()),
    }
    Ok(end.into_messages())
}

async fn execute_portal_inner(
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

    // t_e1c819ad: authorize the portal's statement before it executes.
    if let Err(denied) = authz::authorize(
        &ctx.schema,
        session.auth(),
        &authz::prepared_permissions(&parsed, &ctx.default_schema),
    ) {
        session.mark_txn_failed();
        return vec![session.fail(denied)];
    }

    if let Some(error) = expired_transaction_error(ctx, session) {
        session.mark_txn_failed();
        return vec![session.fail(error)];
    }

    match parsed {
        // Routed to `execute_select_portal` before this point.
        PreparedKind::Select(_) => vec![session.fail(query::error_response(
            "XX000",
            "internal error: a SELECT portal reached the non-streaming path",
        ))],
        // No-FROM expression select: no tables, no params. Evaluate and render.
        PreparedKind::Exprs(items) => {
            let snapshot = read_snapshot(ctx, session);
            let scalar_ctx = scalar_read_ctx(ctx, session, &snapshot);
            match query::execute_scalar_select(&items, scalar_ctx).await {
                Ok(result) => {
                    let msgs = query::render_execute_result(Ok(result), &result_formats);
                    if matches!(msgs.first(), Some(BackendMessage::ErrorResponse { .. })) {
                        session.mark_error();
                    }
                    msgs
                }
                Err(msg) => vec![session.fail(msg)],
            }
        }
        // Parameterized DML over the extended protocol. The bound params drive
        // `$N` substitution; the Execute path omits the leading RowDescription
        // for INSERT RETURNING (the client learned columns from Describe). In an
        // open transaction the write is BUFFERED into the session write-set
        // (`Some(txn_writes_mut())`) and committed atomically via PostgreSQL
        // MVCC at COMMIT; autocommit (no open txn) applies immediately. A failed DML
        // poisons the transaction (`execute_dml`).
        PreparedKind::Insert(ins) => {
            // Extended Execute: no leading RowDescription for RETURNING (sent at
            // Describe time); rows honor the portal's result formats.
            let returning_opts = query::ReturningOpts {
                with_row_description: false,
                result_formats: &result_formats,
            };
            let in_txn = session.in_txn();
            let transaction_writes = if in_txn {
                Some(session.txn_writes_mut())
            } else {
                None
            };
            let msgs = query::execute_insert(
                dml_context(ctx, transaction_writes),
                &ins,
                &params,
                returning_opts,
            )
            .await;
            execute_dml(session, msgs)
        }
        PreparedKind::Update(upd) => {
            let txn = if session.in_txn() {
                Some(session.txn_writes_mut())
            } else {
                None
            };
            let msgs = query::execute_update(dml_context(ctx, txn), &upd, &params).await;
            execute_dml(session, msgs)
        }
        PreparedKind::Delete(del) => {
            let txn = if session.in_txn() {
                Some(session.txn_writes_mut())
            } else {
                None
            };
            let msgs = query::execute_delete(dml_context(ctx, txn), &del, &params).await;
            execute_dml(session, msgs)
        }
    }
}

/// Post-process a DML executor's messages for the extended Execute path: if it
/// produced an `ErrorResponse`, set the skip-until-Sync flag AND poison the
/// transaction (so a failed statement inside a `BEGIN` block can never be
/// committed — the same fail-loud rule the simple-query path applies). Returns
/// the messages unchanged.
fn execute_dml(session: &mut Session, msgs: Vec<BackendMessage>) -> Vec<BackendMessage> {
    if msgs
        .iter()
        .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
    {
        session.mark_error();
        session.mark_txn_failed();
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
    tls: PgTls,
) -> std::io::Result<()>
where
    S: VerifierStore + Send + Sync + 'static,
{
    tracing::info!(
        offers_tls = tls.offers_tls(),
        requires_tls = tls.requires_tls(),
        "PostgreSQL listener TLS posture"
    );
    let tls = Arc::new(tls);
    let _snapshot_reaper = crate::mvcc::MvccManager::spawn_snapshot_reaper(ctx.mvcc.clone());
    // Install the MVCC observer before any connection can run, and via the
    // node's Accord-state slot rather than a committer: at this point the node
    // has not formed a cluster yet, and the observer must be present when it
    // does.
    ctx.accord
        .register_observer(ctx.mvcc.clone())
        .map_err(std::io::Error::other)?;
    loop {
        let (stream, peer) = listener.accept().await?;
        let store = Arc::clone(&store);
        let ctx = Arc::clone(&ctx);
        let tls = Arc::clone(&tls);
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, store, ctx, &tls).await {
                tracing::warn!(%peer, %error, "PostgreSQL connection ended with an I/O error");
            }
        });
    }
}

/// PostgreSQL MVCC transaction behavior (FMEA PG-1): a `BEGIN`/`COMMIT` block
/// buffers its DML and commits through the local MVCC manager. ROLLBACK discards
/// the buffer. These tests do not establish cluster-wide commit ordering.
#[cfg(test)]
pub(crate) mod txn_atomicity_tests {
    use super::*;
    use crate::extended::Session;
    use ferrosa_common::timeuuid::SYNTHETIC_KEY_COLUMN;
    use ferrosa_schema::{
        AuthContext, AuthMethod, ClusteringOrder, ColumnKind, ColumnMetadata, DeploymentMode,
        EnvSecretsProvider, KeyspaceMetadata, PasswordHasher, PasswordPolicy, RateLimitConfig,
        ReplicationParams, SchemaConfig, TableMetadata, TableParams, TestAuditSink,
    };
    use ferrosa_storage::{
        CommitLogConfig, CompactionConfig, StorageEngineConfig, SyncStrategyConfig,
    };
    use indexmap::IndexMap;
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::time::Duration;
    use uuid::Uuid;

    /// The whole reply of a simple query as one value: streamed messages, then
    /// the tail. These tests read small results and want them collected.
    async fn execute_simple(
        ctx: &QueryContext,
        session: &mut Session,
        sql: &str,
    ) -> Vec<BackendMessage> {
        let mut messages: Vec<BackendMessage> = Vec::new();
        let tail = execute_simple_to(ctx, session, sql, &mut messages)
            .await
            .expect("an in-memory sink cannot fail");
        messages.extend(tail);
        messages
    }

    /// The whole reply of an unlimited `Execute`, collected.
    async fn execute_portal(
        ctx: &QueryContext,
        session: &mut Session,
        portal_name: &str,
    ) -> Vec<BackendMessage> {
        let mut messages: Vec<BackendMessage> = Vec::new();
        let tail = execute_portal_to(ctx, session, portal_name, 0, &mut messages)
            .await
            .expect("an in-memory sink cannot fail");
        messages.extend(tail);
        messages
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

    pub(crate) fn superuser() -> AuthContext {
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
            cache_hot_window_secs: 900,
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

    pub(crate) fn ctx_with(engine: Arc<StorageEngine>, schema: Arc<Schema>) -> QueryContext {
        QueryContext {
            engine,
            schema,
            default_schema: "public".to_string(),
            mvcc: Arc::new(MvccManager::default()),
            accord: AccordAccess::disabled(),
            ddl: None,
            jsonb_limits: crate::jsonb_wire::test_limits(),
            portals: Default::default(),
        }
    }

    pub(crate) async fn make_ctx() -> (tempfile::TempDir, QueryContext) {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
        engine.register_table(kv_storage_schema()).unwrap();
        let ctx = ctx_with(Arc::new(engine), Arc::new(schema_with_kv()));
        (dir, ctx)
    }

    /// `make_ctx` for a table whose partition key is the synthetic `_sys_ck_` column —
    /// the shape `ddl::plan_create_table` produces for a `CREATE TABLE` with no
    /// `PRIMARY KEY`. `public.sk(_sys_ck_ uuid, v text)`, keyed on `_sys_ck_`. The key
    /// column is invisible to the client, so every INSERT/COPY row must have one minted.
    pub(crate) async fn make_ctx_synthetic_key() -> (tempfile::TempDir, QueryContext) {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(engine_config(dir.path()), None).unwrap();
        engine
            .register_table(synthetic_key_storage_schema())
            .unwrap();
        let ctx = ctx_with(Arc::new(engine), Arc::new(schema_with_synthetic_key()));
        (dir, ctx)
    }

    /// `make_ctx_synthetic_key` with the transaction write-set cap raised, so a COPY large
    /// enough to matter buffers without hitting the fail-loud `53400` cap first (the deployed
    /// cluster runs `FERROSA_POSTGRES_MAX_TXN_WRITES=3000000` for `pgbench -i`'s ~1.1M-row load).
    pub(crate) async fn make_ctx_synthetic_key_with_cap(
        max_txn_writes: usize,
    ) -> (tempfile::TempDir, QueryContext) {
        let (dir, mut ctx) = make_ctx_synthetic_key().await;
        ctx.mvcc = Arc::new(crate::mvcc::MvccManager::with_max_txn_writes(
            max_txn_writes,
        ));
        (dir, ctx)
    }

    fn synthetic_key_storage_schema() -> ferrosa_common::schema::TableSchema {
        use ferrosa_common::schema::{ColumnDefinition, TableSchema};
        TableSchema {
            keyspace: "public".to_string(),
            table: "sk".to_string(),
            key_type: "org.apache.cassandra.db.marshal.UUIDType".to_string(),
            clustering_columns: vec![],
            static_columns: vec![],
            regular_columns: vec![ColumnDefinition {
                name: "v".to_string(),
                type_name: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            extensions: Default::default(),
        }
    }

    /// `public.sk`, whose partition key is the synthetic `_sys_ck_` column of type `uuid`
    /// — the shape `plan_create_table` produces for a PK-less table.
    fn schema_with_synthetic_key() -> Schema {
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
            SYNTHETIC_KEY_COLUMN.to_string(),
            column(SYNTHETIC_KEY_COLUMN, ColumnKind::PartitionKey, "uuid"),
        );
        cols.insert("v".to_string(), column("v", ColumnKind::Regular, "text"));
        schema
            .create_table(
                TableMetadata {
                    keyspace: "public".to_string(),
                    name: "sk".to_string(),
                    id: Uuid::new_v4(),
                    columns: cols,
                    partition_key: vec![SYNTHETIC_KEY_COLUMN.to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .expect("create table sk");
        schema
    }

    /// Every DISTINCT synthetic key visible through `SELECT _sys_ck_ FROM sk`, as its
    /// 16 raw bytes. The synthetic key is invisible to `SELECT *`; naming it returns it.
    pub(crate) async fn synthetic_keys(ctx: &QueryContext) -> Vec<Vec<u8>> {
        let msgs = query::execute_query(
            &ctx.engine,
            &ctx.schema,
            "SELECT _sys_ck_ FROM sk",
            &ctx.default_schema,
            &ctx.jsonb_limits,
            None,
        )
        .await;
        assert!(
            !msgs
                .iter()
                .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
            "SELECT _sys_ck_ failed: {msgs:?}"
        );
        msgs.iter()
            .filter_map(|m| match m {
                BackendMessage::DataRow { columns } => Some(columns[0].clone().unwrap_or_default()),
                _ => None,
            })
            .collect()
    }

    /// The `v` values visible through `SELECT v FROM sk`, in read order.
    pub(crate) async fn synthetic_values(ctx: &QueryContext) -> Vec<String> {
        let msgs = query::execute_query(
            &ctx.engine,
            &ctx.schema,
            "SELECT v FROM sk",
            &ctx.default_schema,
            &ctx.jsonb_limits,
            None,
        )
        .await;
        msgs.iter()
            .filter_map(|m| match m {
                BackendMessage::DataRow { columns } => Some(
                    String::from_utf8_lossy(columns[0].as_deref().unwrap_or_default()).into_owned(),
                ),
                _ => None,
            })
            .collect()
    }

    /// `make_ctx` with hard write admission enabled, so a client can fill the
    /// buffer and be refused. The flush threshold is raised out of the way so a
    /// background flush cannot drain the memtable mid-test.
    async fn make_ctx_with_write_admission(
        backpressure_bytes: u64,
    ) -> (tempfile::TempDir, QueryContext) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = engine_config(dir.path());
        config.memtable_backpressure_bytes = backpressure_bytes;
        config.flush_threshold_bytes = 1 << 30;
        let engine = StorageEngine::new(config, None).unwrap();
        engine.register_table(kv_storage_schema()).unwrap();
        let ctx = ctx_with(Arc::new(engine), Arc::new(schema_with_kv()));
        (dir, ctx)
    }

    /// Rows visible for key `k`, read back through the `execute_query` SELECT
    /// path with no transaction buffer.
    pub(crate) async fn row_count(ctx: &QueryContext, key: &str) -> usize {
        let msgs = query::execute_query(
            &ctx.engine,
            &ctx.schema,
            &format!("SELECT k FROM kv WHERE k = '{key}'"),
            &ctx.default_schema,
            &ctx.jsonb_limits,
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

    /// The SQLSTATE of the sole `ErrorResponse` in a reply, if any.
    fn error_sqlstate(messages: &[BackendMessage]) -> Option<String> {
        messages.iter().find_map(|m| match m {
            BackendMessage::ErrorResponse { fields } => fields
                .iter()
                .find(|(k, _)| *k == b'C')
                .map(|(_, v)| v.to_string()),
            _ => None,
        })
    }

    fn command_tag(messages: &[BackendMessage]) -> Option<String> {
        messages.iter().find_map(|m| match m {
            BackendMessage::CommandComplete { tag } => Some(tag.clone()),
            _ => None,
        })
    }

    /// `TRUNCATE` is a normal replicated WRITE of a table-level tombstone: the
    /// row disappears immediately, and the mechanism is a tombstone written into
    /// the table's own store — NOT a node-local `StorageEngine::truncate`, which
    /// would leave no marker (and would empty only this replica).
    #[tokio::test]
    async fn truncate_writes_a_table_tombstone_not_a_local_truncate() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        execute_simple(
            &ctx,
            &mut session,
            "INSERT INTO kv (k, v) VALUES ('k1', 'v')",
        )
        .await;
        assert_eq!(row_count(&ctx, "k1").await, 1);

        let messages = execute_simple(&ctx, &mut session, "TRUNCATE TABLE kv").await;
        assert_eq!(command_tag(&messages).as_deref(), Some("TRUNCATE TABLE"));
        // Logically immediate: the row is gone at once.
        assert_eq!(row_count(&ctx, "k1").await, 0);

        // The mechanism is a write: the reserved table-tombstone partition is
        // present in the table's store with a non-LIVE deletion. A node-local
        // `StorageEngine::truncate` empties the store and would leave no marker.
        let table_id = ferrosa_storage::TableId::new("public", "kv");
        let marker = ctx
            .engine
            .read_limited_rows(
                &table_id,
                &ferrosa_storage::table_tombstone::table_tombstone_key(),
                0,
            )
            .expect("read the tombstone partition")
            .expect("the table tombstone must have been written");
        assert!(
            !marker.deletion.is_live(),
            "TRUNCATE must write a table tombstone, not empty a replica"
        );

        // A stale copy of the pre-truncate row, re-inserted at its ORIGINAL older
        // timestamp (what a repair from a stale replica would do), stays invisible.
        execute_simple(
            &ctx,
            &mut session,
            "INSERT INTO kv (k, v) VALUES ('k1', 'v')",
        )
        .await;
        // The re-insert is newer than the tombstone, so it is NOT the stale-copy
        // case; the storage-level no-resurrection test covers the older copy. Here
        // we only pin that the newly written row is visible (property 4).
        assert_eq!(row_count(&ctx, "k1").await, 1);
    }

    /// A table that does not exist is `42P01`, and the truncate applies nothing.
    #[tokio::test]
    async fn truncate_of_a_missing_table_is_refused_42p01() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        let messages = execute_simple(&ctx, &mut session, "TRUNCATE TABLE nope").await;
        assert_eq!(error_sqlstate(&messages).as_deref(), Some("42P01"));
    }

    /// In CLUSTER mode an autocommit `TRUNCATE` must be routed through the cluster
    /// commit path — NOT applied to local storage directly.
    ///
    /// `TRUNCATE` is a replicated write; if it were omitted from the "data
    /// statement" set it would never enter the implicit-transaction/Accord path
    /// and the marker would be written locally only, so no cluster replication (and
    /// no `ConsistencyLevel::All` commit) could ever govern it. This pins the
    /// routing by asserting the tombstone reaches the committer, and that the
    /// front-end did not write the marker to its own storage.
    #[tokio::test]
    async fn cluster_autocommit_truncate_is_routed_through_the_cluster_not_local_only() {
        let (_dir, mut ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        execute_simple(
            &ctx,
            &mut session,
            "INSERT INTO kv (k, v) VALUES ('k1', 'v')",
        )
        .await;

        let committer =
            std::sync::Arc::new(ferrosa_storage::accord::MockTransactionCommitter::new());
        ctx.accord = AccordAccess::fixed(committer.clone());

        let messages = execute_simple(&ctx, &mut session, "TRUNCATE TABLE kv").await;
        assert_eq!(command_tag(&messages).as_deref(), Some("TRUNCATE TABLE"));

        // The tombstone reached the cluster committer: an autocommit TRUNCATE is
        // wrapped in an implicit transaction and committed via the cluster path.
        let tombstone_key = ferrosa_storage::table_tombstone::table_tombstone_key()
            .key
            .as_bytes()
            .to_vec();
        let routed = committer
            .committed()
            .iter()
            .flatten()
            .any(|w| w.key == tombstone_key);
        assert!(
            routed,
            "an autocommit TRUNCATE on a cluster must commit through the cluster \
             (its tombstone must reach the committer), not write local-only"
        );

        // And the front-end did NOT apply the marker to its own storage — the
        // write is the cluster's to replicate and apply.
        let table_id = ferrosa_storage::TableId::new("public", "kv");
        let local_marker = ctx
            .engine
            .read_limited_rows(
                &table_id,
                &ferrosa_storage::table_tombstone::table_tombstone_key(),
                0,
            )
            .expect("read the tombstone partition");
        assert!(
            local_marker.is_none(),
            "with a committer present the front-end must not apply the truncate \
             marker locally; that would bypass cluster replication"
        );
    }

    /// TRUNCATE is transactional: inside a transaction it is accepted (no `25001`),
    /// applies on COMMIT, and is discarded by ROLLBACK.
    #[tokio::test]
    async fn truncate_applies_on_commit_and_is_discarded_by_rollback() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        execute_simple(
            &ctx,
            &mut session,
            "INSERT INTO kv (k, v) VALUES ('k1', 'v')",
        )
        .await;
        assert_eq!(row_count(&ctx, "k1").await, 1);

        // Inside a transaction TRUNCATE is accepted.
        execute_simple(&ctx, &mut session, "BEGIN").await;
        let messages = execute_simple(&ctx, &mut session, "TRUNCATE TABLE kv").await;
        assert_eq!(
            error_sqlstate(&messages),
            None,
            "TRUNCATE must be accepted inside a transaction now: {messages:?}"
        );
        // Buffered, not applied yet.
        assert_eq!(row_count(&ctx, "k1").await, 1);
        // ROLLBACK discards it.
        execute_simple(&ctx, &mut session, "ROLLBACK").await;
        assert_eq!(
            row_count(&ctx, "k1").await,
            1,
            "ROLLBACK must discard the buffered truncate"
        );

        // COMMIT applies it.
        execute_simple(&ctx, &mut session, "BEGIN").await;
        execute_simple(&ctx, &mut session, "TRUNCATE TABLE kv").await;
        execute_simple(&ctx, &mut session, "COMMIT").await;
        assert_eq!(
            row_count(&ctx, "k1").await,
            0,
            "COMMIT must apply the buffered truncate"
        );
    }

    /// A PK-less table keys on the invisible synthetic `_sys_ck_` uuid. `TRUNCATE`
    /// buffers a table-level tombstone under a RESERVED partition key — a
    /// partition-tombstone MARKER, not a row — and building a transaction's row
    /// images must skip it. Before the fix, COMMIT decoded the marker's magic bytes
    /// as `_sys_ck_` (a uuid) and failed loud (`build transaction row image failed:
    /// uuid requires 16 bytes`), losing the whole transaction. This is the exact
    /// shape `pgbench -i` hits: it runs `TRUNCATE` INSIDE the load transaction, so
    /// the deployed load died at COMMIT even though every row had landed. The `kv`
    /// test above could not see this because a `text` key accepts any bytes.
    #[tokio::test]
    async fn truncate_inside_a_transaction_commits_on_a_pkless_table() {
        let (_dir, ctx) = make_ctx_synthetic_key().await;
        let mut session = Session::new(superuser());
        execute_simple(&ctx, &mut session, "INSERT INTO sk (v) VALUES ('a')").await;
        execute_simple(&ctx, &mut session, "INSERT INTO sk (v) VALUES ('b')").await;
        assert_eq!(synthetic_values(&ctx).await.len(), 2);

        execute_simple(&ctx, &mut session, "BEGIN").await;
        let messages = execute_simple(&ctx, &mut session, "TRUNCATE TABLE sk").await;
        assert_eq!(
            error_sqlstate(&messages),
            None,
            "TRUNCATE is accepted inside a transaction: {messages:?}"
        );
        let messages = execute_simple(&ctx, &mut session, "COMMIT").await;
        assert_eq!(
            error_sqlstate(&messages),
            None,
            "COMMIT must build row images for the transaction's ROWS, never for the \
             reserved table-tombstone key: {messages:?}"
        );
        assert_eq!(command_tag(&messages).as_deref(), Some("COMMIT"));
        assert!(
            synthetic_values(&ctx).await.is_empty(),
            "the buffered truncate must apply on COMMIT"
        );
    }

    /// The same reserved tombstone key reaches the open transaction's READ overlay:
    /// a SELECT in the transaction that buffered the TRUNCATE must not decode it as a
    /// data row either. Before the fix this failed loud (`transaction overlay failed:
    /// uuid requires 16 bytes`).
    #[tokio::test]
    async fn reading_inside_a_transaction_after_a_truncate_does_not_decode_the_tombstone() {
        let (_dir, ctx) = make_ctx_synthetic_key().await;
        let mut session = Session::new(superuser());
        execute_simple(&ctx, &mut session, "INSERT INTO sk (v) VALUES ('a')").await;

        execute_simple(&ctx, &mut session, "BEGIN").await;
        execute_simple(&ctx, &mut session, "TRUNCATE TABLE sk").await;
        let messages = execute_simple(&ctx, &mut session, "SELECT v FROM sk").await;
        assert_eq!(
            error_sqlstate(&messages),
            None,
            "the transaction overlay must skip the reserved tombstone key: {messages:?}"
        );
        execute_simple(&ctx, &mut session, "COMMIT").await;
    }

    /// An autocommit `TRUNCATE` on a PK-less table is an implicit transaction and
    /// takes the same row-image path; it must commit too. Before the fix it failed
    /// with `write failed: invalid data: build transaction row image failed: uuid
    /// requires 16 bytes`.
    #[tokio::test]
    async fn autocommit_truncate_on_a_pkless_table_commits() {
        let (_dir, ctx) = make_ctx_synthetic_key().await;
        let mut session = Session::new(superuser());
        execute_simple(&ctx, &mut session, "INSERT INTO sk (v) VALUES ('a')").await;

        let messages = execute_simple(&ctx, &mut session, "TRUNCATE TABLE sk").await;
        assert_eq!(
            error_sqlstate(&messages),
            None,
            "an autocommit TRUNCATE on a PK-less table must commit: {messages:?}"
        );
        assert_eq!(command_tag(&messages).as_deref(), Some("TRUNCATE TABLE"));
        assert!(
            synthetic_values(&ctx).await.is_empty(),
            "the truncate applied"
        );
    }

    /// A `TRUNCATE` inside a transaction must also commit when a table tombstone
    /// already exists in storage (from an earlier commit): the BEFORE-image read the
    /// commit performs must skip the reserved marker key too, not only the after-image
    /// build. Otherwise a second truncate of the same PK-less table fails loud
    /// (`read before image failed: uuid requires 16 bytes`).
    #[tokio::test]
    async fn truncate_inside_a_transaction_commits_when_a_tombstone_already_exists() {
        let (_dir, ctx) = make_ctx_synthetic_key().await;
        let mut session = Session::new(superuser());
        // Autocommit: the marker lands in storage.
        let messages = execute_simple(&ctx, &mut session, "TRUNCATE TABLE sk").await;
        assert_eq!(
            error_sqlstate(&messages),
            None,
            "first truncate: {messages:?}"
        );

        execute_simple(&ctx, &mut session, "BEGIN").await;
        execute_simple(&ctx, &mut session, "TRUNCATE TABLE sk").await;
        let messages = execute_simple(&ctx, &mut session, "COMMIT").await;
        assert_eq!(
            error_sqlstate(&messages),
            None,
            "a second truncate of the same PK-less table must commit: {messages:?}"
        );
    }

    /// VACUUM / VACUUM FULL / VACUUM ANALYZE are accepted and answered with the tag a client
    /// expects; the rows survive, because flushing and compacting is not destructive. ANALYZE
    /// really is a no-op: no statistics are collected, and that is stated in the dispatch arm
    /// rather than being hidden here.
    #[tokio::test]
    async fn vacuum_flushes_and_submits_compaction_without_touching_rows() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        execute_simple(
            &ctx,
            &mut session,
            "INSERT INTO kv (k, v) VALUES ('k1', 'v')",
        )
        .await;
        for (sql, tag) in [
            ("VACUUM", "VACUUM"),
            ("VACUUM FULL", "VACUUM"),
            ("VACUUM FULL ANALYZE", "VACUUM"),
            ("VACUUM ANALYZE kv", "VACUUM"),
            ("ANALYZE kv", "ANALYZE"),
        ] {
            let messages = execute_simple(&ctx, &mut session, sql).await;
            assert_eq!(
                error_sqlstate(&messages),
                None,
                "`{sql}` must succeed, got {messages:?}"
            );
            assert_eq!(command_tag(&messages).as_deref(), Some(tag), "`{sql}` tag");
        }
        assert_eq!(
            row_count(&ctx, "k1").await,
            1,
            "VACUUM flushes and compacts; it must not drop a live row"
        );
    }

    /// VACUUM resolves the table it names. Accept-and-report ignored the table entirely, so a
    /// `VACUUM` against a relation that does not exist used to succeed; now it is refused, which
    /// is the assertion that the statement executes rather than only answering.
    #[tokio::test]
    async fn vacuum_on_a_missing_table_is_refused() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        let messages = execute_simple(&ctx, &mut session, "VACUUM nope").await;
        assert!(
            format!("{messages:?}").contains("42P01"),
            "VACUUM must resolve the relation it names; got {messages:?}"
        );
    }

    /// At the suspended-portal limit, a fresh portal executed with `max_rows`
    /// is refused BEFORE any `DataRow`, as PostgreSQL refuses a resource limit
    /// before output: the client never sees partial rows followed by 53000.
    #[tokio::test]
    async fn a_portal_over_the_suspension_limit_is_refused_before_any_row() {
        let (_dir, mut ctx) = make_ctx().await;
        ctx.portals = Arc::new(crate::SuspendedPortals::new(crate::PortalLimits {
            per_connection: 1,
            per_node: 100,
            idle_timeout: Duration::from_secs(600),
        }));
        let mut writer = Session::new(superuser());
        for k in ["a", "b", "c", "d"] {
            execute_simple(
                &ctx,
                &mut writer,
                &format!("INSERT INTO kv (k, v) VALUES ('{k}', 'v')"),
            )
            .await;
        }
        let mut s = Session::new(superuser());
        execute_simple(&ctx, &mut s, "BEGIN").await;
        s.on_parse("st".into(), "SELECT k FROM kv", vec![]);
        for portal in ["p1", "p2"] {
            s.on_bind(
                portal.into(),
                "st".into(),
                &[],
                &[],
                vec![],
                &crate::jsonb_wire::test_limits(),
            );
        }
        async fn execute(ctx: &QueryContext, s: &mut Session, portal: &str) -> Vec<BackendMessage> {
            let mut messages: Vec<BackendMessage> = Vec::new();
            let tail = execute_portal_to(ctx, s, portal, 1, &mut messages)
                .await
                .expect("an in-memory sink cannot fail");
            messages.extend(tail);
            messages
        }
        let first = execute(&ctx, &mut s, "p1").await;
        assert!(
            matches!(first.last(), Some(BackendMessage::PortalSuspended)),
            "the first portal suspends: {first:?}"
        );
        let second = execute(&ctx, &mut s, "p2").await;
        assert!(is_error(&second, "53000"), "refused with 53000: {second:?}");
        assert!(
            !second
                .iter()
                .any(|m| matches!(m, BackendMessage::DataRow { .. })),
            "a refused portal sent rows before its error: {second:?}"
        );
        assert_eq!(ctx.portals.suspended(), 1);
        ctx.engine.shutdown().unwrap();
    }

    /// A DML portal that has run to completion is never applied twice: as in
    /// PostgreSQL, a second `Execute` returns its completion tag and does not
    /// re-run the statement. An INSERT here is an upsert, so the row count
    /// alone cannot show a second apply (it would still silently overwrite a
    /// concurrent writer); inside a transaction every apply is buffered, so
    /// the write-set shows it.
    #[tokio::test]
    async fn a_completed_dml_portal_is_not_applied_twice() {
        let (_dir, ctx) = make_ctx().await;
        let mut s = Session::new(superuser());
        execute_simple(&ctx, &mut s, "BEGIN").await;
        s.on_parse(
            "ins".into(),
            "INSERT INTO kv (k, v) VALUES ($1, 'v')",
            vec![25],
        );
        s.on_bind(
            "p".into(),
            "ins".into(),
            &[0],
            &[Some(b"once".to_vec())],
            vec![],
            &crate::jsonb_wire::test_limits(),
        );
        let first = execute_portal(&ctx, &mut s, "p").await;
        let second = execute_portal(&ctx, &mut s, "p").await;
        assert_eq!(
            s.txn_writes().len(),
            1,
            "the second Execute applied the INSERT again"
        );
        execute_simple(&ctx, &mut s, "COMMIT").await;
        assert_eq!(row_count(&ctx, "once").await, 1, "inserted exactly once");
        let tag = |msgs: &[BackendMessage]| -> Option<String> {
            msgs.iter().find_map(|m| match m {
                BackendMessage::CommandComplete { tag } => Some(tag.clone()),
                _ => None,
            })
        };
        assert!(
            tag(&first).is_some(),
            "the first Execute completes: {first:?}"
        );
        assert_eq!(
            second,
            vec![BackendMessage::CommandComplete {
                tag: tag(&first).expect("a tag")
            }],
            "the second Execute only repeats the completion"
        );
        ctx.engine.shutdown().unwrap();
    }

    fn is_error(msgs: &[BackendMessage], code: &str) -> bool {
        msgs.iter().any(|m| {
            matches!(m, BackendMessage::ErrorResponse { fields }
                if fields.iter().any(|f| *f == (b'C', code.to_string())))
        })
    }

    /// Records how many messages each `send` carried.
    #[derive(Default)]
    struct BatchRecorder {
        sends: Vec<usize>,
        data_rows: usize,
    }

    impl ReplySink for BatchRecorder {
        async fn send(&mut self, messages: Vec<BackendMessage>) -> std::io::Result<()> {
            self.sends.push(messages.len());
            self.data_rows += messages
                .iter()
                .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
                .count();
            Ok(())
        }
    }

    /// The simple protocol streams: a `SELECT` reaches the sink as several
    /// bounded batches, never as one message list holding the whole result.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn simple_select_streams_rows_in_bounded_batches() {
        const TOTAL: usize = 200;
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        for i in 0..TOTAL {
            let sql = format!("INSERT INTO kv (k, v) VALUES ('k{i:04}', 'v')");
            let reply = execute_simple(&ctx, &mut session, &sql).await;
            assert!(
                !reply
                    .iter()
                    .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
                "insert {i} failed: {reply:?}"
            );
        }

        let mut sink = BatchRecorder::default();
        let tail = execute_simple_to(&ctx, &mut session, "SELECT k, v FROM kv", &mut sink)
            .await
            .expect("in-memory sink cannot fail");

        assert_eq!(sink.data_rows, TOTAL, "every row is streamed");
        assert!(
            matches!(&tail[..], [BackendMessage::CommandComplete { tag }] if tag == &format!("SELECT {TOTAL}")),
            "the tail is the completion, not the rows: {tail:?}"
        );
        let largest = sink.sends.iter().copied().max().unwrap_or(0);
        assert!(
            largest <= crate::result_stream::RESULT_BATCH_ROWS,
            "a single send carried {largest} messages; batches are bounded"
        );
        assert!(sink.sends.len() > TOTAL / crate::result_stream::RESULT_BATCH_ROWS);
    }

    #[tokio::test]
    async fn rollback_discards_buffered_writes() {
        // BEGIN; INSERT (buffered); ROLLBACK ⇒ the row was never applied.
        // Contrast: an autocommit INSERT IS applied.
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());

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
    async fn commit_uses_postgres_mvcc_without_accord() {
        // PostgreSQL owns its MVCC commit path in standalone and cluster modes.
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());

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
            matches!(&m[..], [BackendMessage::CommandComplete { tag }] if tag == "COMMIT"),
            "PostgreSQL MVCC commit must not require the Cassandra Accord committer: {m:?}"
        );
        assert!(
            !session.in_txn(),
            "the transaction is ended even on failure"
        );
        assert_eq!(
            row_count(&ctx, "x").await,
            1,
            "the PostgreSQL MVCC commit applies the buffered row"
        );

        ctx.engine.shutdown().unwrap();
    }

    /// An explicit `BEGIN; INSERT; COMMIT` refused for write pressure must say
    /// `53000`, like every other refusal.
    ///
    /// Found by review: the autocommit paths were fixed first, but the FINAL
    /// outcome arm of `commit_txn` — the one every non-empty explicit
    /// transaction lands on — still answered `58000 system_error`
    /// unconditionally. A transactional client was therefore told its commit
    /// hit a system fault when the server was simply refusing the pace, and
    /// would not retry. The two autocommit tests could not catch it because
    /// they never open a transaction.
    #[tokio::test]
    async fn a_refused_explicit_commit_says_insufficient_resources() {
        const BACKPRESSURE_BYTES: u64 = 64 * 1024;
        let (_dir, ctx) = make_ctx_with_write_admission(BACKPRESSURE_BYTES).await;
        let mut session = Session::new(superuser());

        // Fill the memtable with autocommit writes first. The transaction's own
        // write-set is capped (`max_txn_writes` -> 53400), so the pressure has
        // to be built outside the transaction, then the COMMIT's write is what
        // crosses the admission threshold.
        let payload = "x".repeat(1024);
        let mut filled = false;
        for seq in 0..4096u32 {
            let sql = format!("INSERT INTO kv (k, v) VALUES ('fill-{seq}', '{payload}')");
            let msgs = execute_simple(&ctx, &mut session, &sql).await;
            if msgs
                .iter()
                .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
            {
                filled = true;
                break;
            }
        }
        assert!(
            filled,
            "the memtable must reach its admission threshold before the transaction"
        );

        execute_simple(&ctx, &mut session, "BEGIN").await;
        execute_simple(
            &ctx,
            &mut session,
            &format!("INSERT INTO kv (k, v) VALUES ('txn-row', '{payload}')"),
        )
        .await;
        let msgs = execute_simple(&ctx, &mut session, "COMMIT").await;

        let code = msgs
            .iter()
            .find_map(|m| match m {
                BackendMessage::ErrorResponse { fields } => Some(fields[1].1.clone()),
                _ => None,
            })
            .expect("the COMMIT must be refused while the buffer is full");
        assert_eq!(
            code, "53000",
            "a commit refused for backpressure must be retryable insufficient_resources, \
             not 58000 system_error: {msgs:?}"
        );

        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn commit_applies_postgres_write_set() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());

        execute_simple(&ctx, &mut session, "BEGIN").await;
        execute_simple(
            &ctx,
            &mut session,
            "INSERT INTO kv (k, v) VALUES ('y', 'committed')",
        )
        .await;
        execute_simple(
            &ctx,
            &mut session,
            "INSERT INTO kv (k, v) VALUES ('y2', 'committed-too')",
        )
        .await;
        let m = execute_simple(&ctx, &mut session, "COMMIT").await;
        assert!(
            matches!(&m[..], [BackendMessage::CommandComplete { tag }] if tag == "COMMIT"),
            "COMMIT acks via the PostgreSQL MVCC path: {m:?}"
        );
        assert!(!session.in_txn());
        assert_eq!(row_count(&ctx, "y").await, 1);
        assert_eq!(row_count(&ctx, "y2").await, 1);

        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn multi_row_snapshots_hide_partial_replica_apply() {
        let (_dir, ctx) = make_ctx().await;
        let mut seed = Session::new(superuser());
        for (key, value) in [("row-a", "before-a"), ("row-b", "before-b")] {
            let insert = format!("INSERT INTO kv (k, v) VALUES ('{key}', '{value}')");
            execute_simple(&ctx, &mut seed, &insert).await;
        }

        let before_ts = ferrosa_common::accord::Timestamp::synthetic(19);
        let commit_ts = ferrosa_common::accord::Timestamp::synthetic(20);
        let mut writer = Session::new(superuser());
        writer.begin_txn(
            Some(ferrosa_sql::IsolationLevel::Serializable),
            ctx.mvcc.snapshot_with_cluster_ts(before_ts),
        );
        execute_simple(
            &ctx,
            &mut writer,
            "UPDATE kv SET v = 'after-a' WHERE k = 'row-a'",
        )
        .await;
        execute_simple(
            &ctx,
            &mut writer,
            "UPDATE kv SET v = 'after-b' WHERE k = 'row-b'",
        )
        .await;
        let mutations: Vec<_> = writer
            .take_txn_writes()
            .into_iter()
            .map(|write| write.0)
            .collect();
        assert_eq!(mutations.len(), 2);

        let changes = query::prepare_row_changes(&ctx.engine, &ctx.schema, &mutations).unwrap();
        let metadata = serde_json::to_vec(&changes).unwrap();
        let txn_id =
            ferrosa_common::accord::TxnId::new(1, ferrosa_common::accord::Timestamp::synthetic(21));
        let metadata_batch = [metadata];
        <MvccManager as ferrosa_storage::accord::PostgresMvccApplyObserver>::prepare_postgres_apply(
            &ctx.mvcc,
            txn_id,
            commit_ts,
            &metadata_batch,
        )
        .unwrap();

        // Pause at the replica's storage seam after the complete MVCC row-image
        // set is staged but only the first partition has reached storage.
        ctx.engine
            .write_atomic_batch(vec![mutations[0].clone()])
            .unwrap();

        let old_snapshot = ctx.mvcc.snapshot_with_cluster_ts(before_ts);
        let committed_snapshot = ctx.mvcc.snapshot_with_cluster_ts(commit_ts);
        let rows_for = |messages: &[BackendMessage]| {
            messages
                .iter()
                .filter_map(|message| match message {
                    BackendMessage::DataRow { columns } if columns.len() == 2 => {
                        let key = columns[0].as_ref()?.clone();
                        let value = columns[1].as_ref()?.clone();
                        Some((String::from_utf8(key).ok()?, String::from_utf8(value).ok()?))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        async fn read_at(
            ctx: &QueryContext,
            snapshot: &crate::mvcc::MvccSnapshot,
        ) -> Vec<BackendMessage> {
            query::execute_query_with_mvcc(
                read_env(ctx, snapshot),
                "SELECT k, v FROM kv ORDER BY k",
                None,
            )
            .await
        }

        let old_rows = read_at(&ctx, &old_snapshot).await;
        assert_eq!(
            rows_for(&old_rows),
            vec![
                ("row-a".into(), "before-a".into()),
                ("row-b".into(), "before-b".into())
            ]
        );
        let committed_rows = read_at(&ctx, &committed_snapshot).await;
        assert_eq!(
            rows_for(&committed_rows),
            vec![
                ("row-a".into(), "after-a".into()),
                ("row-b".into(), "after-b".into())
            ]
        );

        ctx.engine
            .write_atomic_batch(vec![mutations[1].clone()])
            .unwrap();
        <MvccManager as ferrosa_storage::accord::PostgresMvccApplyObserver>::on_postgres_apply(
            &ctx.mvcc,
            txn_id,
            commit_ts,
            &metadata_batch,
        )
        .unwrap();
        ctx.engine.shutdown().unwrap();
    }

    /// The cluster COMMIT now builds its Accord write-set in ONE streaming pass
    /// (`prepare_accord_writes`) instead of a whole-table `Vec<RowChange>` plus a
    /// `changes_by_partition` re-grouping. The output must be **identical** to the
    /// old path, partition by partition: same write order, same storage bytes, and
    /// the same JSON row-version metadata attached to each partition.
    #[tokio::test]
    async fn streaming_accord_writes_match_prepare_row_changes_per_partition() {
        let (_dir, ctx) = make_ctx().await;
        let mut writer = Session::new(superuser());
        execute_simple(&ctx, &mut writer, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        for i in 0..8 {
            execute_simple(
                &ctx,
                &mut writer,
                &format!("INSERT INTO kv (k, v) VALUES ('stream-{i}', 'v-{i}')"),
            )
            .await;
        }
        let mutations: Vec<_> = writer
            .take_txn_writes()
            .into_iter()
            .map(|write| write.0)
            .collect();
        assert_eq!(mutations.len(), 8);

        // Old path: whole-table row images, then grouped by partition bytes.
        let changes = query::prepare_row_changes(&ctx.engine, &ctx.schema, &mutations).unwrap();
        let mut by_partition: std::collections::HashMap<Vec<u8>, Vec<crate::mvcc::RowChange>> =
            std::collections::HashMap::new();
        for change in changes {
            by_partition
                .entry(change.partition_key.clone())
                .or_default()
                .push(change);
        }

        // New path: one streaming pass, no whole-table map.
        let writes =
            query::prepare_accord_writes(&ctx.engine, &ctx.schema, mutations.clone()).unwrap();
        assert_eq!(
            writes.len(),
            mutations.len(),
            "one Accord write per buffered mutation, in order"
        );

        for (write, mutation) in writes.iter().zip(&mutations) {
            let partition_key = mutation.key.key.as_bytes().to_vec();
            assert_eq!(
                write.key, partition_key,
                "write order must follow mutation order"
            );
            let expected = by_partition
                .get(&partition_key)
                .expect("every buffered partition has a row-version entry");

            let (storage, metadata) =
                ferrosa_storage::accord::decode_postgres_mvcc_mutation(&write.mutation)
                    .expect("the streaming path must emit a valid MVCC envelope");
            let metadata = metadata.expect("a data row partition carries row-version metadata");
            let got: Vec<crate::mvcc::RowChange> = serde_json::from_slice(metadata).unwrap();
            assert_eq!(
                serde_json::to_string(&got).unwrap(),
                serde_json::to_string(expected).unwrap(),
                "per-partition row-version metadata must be byte-identical to the whole-table path"
            );

            let mut bytes = vec![0; mutation.serialized_size()];
            mutation.serialize_into(&mut bytes);
            assert_eq!(
                storage,
                bytes.as_slice(),
                "the storage mutation must survive the streaming path unchanged"
            );
        }
        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn committed_transaction_survives_restart_and_uncommitted_buffer_does_not() {
        let (dir, ctx) = make_ctx().await;
        let mut committed = Session::new(superuser());
        execute_simple(&ctx, &mut committed, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        execute_simple(
            &ctx,
            &mut committed,
            "INSERT INTO kv (k, v) VALUES ('restart-committed', 'durable')",
        )
        .await;
        let commit = execute_simple(&ctx, &mut committed, "COMMIT").await;
        assert!(
            matches!(&commit[..], [BackendMessage::CommandComplete { tag }] if tag == "COMMIT"),
            "the transaction must commit before restart: {commit:?}"
        );

        let mut abandoned = Session::new(superuser());
        execute_simple(&ctx, &mut abandoned, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        execute_simple(
            &ctx,
            &mut abandoned,
            "INSERT INTO kv (k, v) VALUES ('restart-uncommitted', 'must-not-appear')",
        )
        .await;
        drop(abandoned);

        ctx.engine.shutdown().unwrap();
        let recovered_engine =
            Arc::new(StorageEngine::new(engine_config(dir.path()), None).unwrap());
        recovered_engine
            .register_table(kv_storage_schema())
            .unwrap();
        let recovered_ctx = ctx_with(recovered_engine, ctx.schema.clone());

        assert_eq!(row_count(&recovered_ctx, "restart-committed").await, 1);
        assert_eq!(row_count(&recovered_ctx, "restart-uncommitted").await, 0);
        recovered_ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn failed_storage_preflight_rejects_the_entire_postgres_write_set() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        execute_simple(&ctx, &mut session, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        execute_simple(
            &ctx,
            &mut session,
            "INSERT INTO kv (k, v) VALUES ('atomic-good', 'must-not-commit-alone')",
        )
        .await;

        let oversized_value = "x".repeat(300 * 1024);
        let oversized_insert =
            format!("INSERT INTO kv (k, v) VALUES ('atomic-oversized', '{oversized_value}')");
        let staged = execute_simple(&ctx, &mut session, &oversized_insert).await;
        assert!(
            matches!(&staged[..], [BackendMessage::CommandComplete { tag }] if tag == "INSERT 0 1"),
            "the oversized row remains buffered until COMMIT: {staged:?}"
        );

        let commit = execute_simple(&ctx, &mut session, "COMMIT").await;
        assert!(
            is_error(&commit, "58000"),
            "storage preflight failure must fail the commit: {commit:?}"
        );
        assert!(!session.in_txn());
        assert_eq!(row_count(&ctx, "atomic-good").await, 0);
        assert_eq!(row_count(&ctx, "atomic-oversized").await, 0);

        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn failed_txn_commit_does_not_apply() {
        // A statement that errors inside a txn poisons it; subsequent DML hits
        // 25P02; COMMIT is treated as ROLLBACK and nothing is applied.
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());

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
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
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

    #[tokio::test]
    async fn transaction_reads_keep_the_begin_snapshot_after_a_concurrent_commit() {
        let (_dir, ctx) = make_ctx().await;
        let mut reader = Session::new(superuser());
        let mut writer = Session::new(superuser());

        let inserted = execute_simple(
            &ctx,
            &mut writer,
            "INSERT INTO kv (k, v) VALUES ('snapshot-row', 'before')",
        )
        .await;
        assert!(
            matches!(&inserted[..], [BackendMessage::CommandComplete { tag }] if tag == "INSERT 0 1"),
            "fixture insert must succeed: {inserted:?}"
        );

        let begin = execute_simple(&ctx, &mut reader, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        assert!(
            matches!(&begin[..], [BackendMessage::CommandComplete { tag }] if tag == "BEGIN"),
            "serializable transaction must start: {begin:?}"
        );
        let read_value = |messages: &[BackendMessage]| {
            messages.iter().find_map(|message| match message {
                BackendMessage::DataRow { columns } => columns
                    .first()
                    .and_then(Option::as_ref)
                    .and_then(|bytes| String::from_utf8(bytes.clone()).ok()),
                _ => None,
            })
        };
        let before = execute_simple(
            &ctx,
            &mut reader,
            "SELECT v FROM kv WHERE k = 'snapshot-row'",
        )
        .await;
        assert_eq!(read_value(&before).as_deref(), Some("before"));

        let updated = execute_simple(
            &ctx,
            &mut writer,
            "UPDATE kv SET v = 'after' WHERE k = 'snapshot-row'",
        )
        .await;
        assert!(
            matches!(&updated[..], [BackendMessage::CommandComplete { tag }] if tag == "UPDATE 1"),
            "concurrent update must commit: {updated:?}"
        );

        let after = execute_simple(
            &ctx,
            &mut reader,
            "SELECT v FROM kv WHERE k = 'snapshot-row'",
        )
        .await;
        assert_eq!(
            read_value(&after).as_deref(),
            Some("before"),
            "a transaction must read from its BEGIN snapshot after another session commits"
        );

        execute_simple(&ctx, &mut reader, "ROLLBACK").await;
        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn expired_transaction_rejects_followup_read_instead_of_using_current_rows() {
        let (_dir, ctx) = make_ctx().await;
        let mut reader = Session::new(superuser());
        let mut writer = Session::new(superuser());

        execute_simple(
            &ctx,
            &mut writer,
            "INSERT INTO kv (k, v) VALUES ('expired-read', 'before')",
        )
        .await;
        execute_simple(&ctx, &mut reader, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        execute_simple(
            &ctx,
            &mut writer,
            "UPDATE kv SET v = 'after' WHERE k = 'expired-read'",
        )
        .await;

        assert_eq!(ctx.mvcc.expire_all_snapshots_for_test(), 1);
        let messages = execute_simple(
            &ctx,
            &mut reader,
            "SELECT v FROM kv WHERE k = 'expired-read'",
        )
        .await;
        assert!(
            is_error(&messages, "40001"),
            "an expired transaction must fail instead of reading the newer row: {messages:?}"
        );
        assert!(reader.in_failed_txn());

        execute_simple(&ctx, &mut reader, "ROLLBACK").await;
        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn expired_read_only_transaction_cannot_commit_successfully() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        execute_simple(&ctx, &mut session, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        assert_eq!(ctx.mvcc.expire_all_snapshots_for_test(), 1);

        let messages = execute_simple(&ctx, &mut session, "COMMIT").await;
        assert!(
            is_error(&messages, "40001"),
            "expired read-only transactions must fail serialization validation: {messages:?}"
        );
        assert!(!session.in_txn(), "the failed transaction is ended");

        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn expired_transaction_rejects_extended_protocol_dml_before_buffering() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        execute_simple(&ctx, &mut session, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        assert_eq!(ctx.mvcc.expire_all_snapshots_for_test(), 1);

        assert!(matches!(
            session.on_parse(
                "insert".to_string(),
                "INSERT INTO kv (k, v) VALUES ($1, 'late-write')",
                vec![25],
            ),
            BackendMessage::ParseComplete
        ));
        assert!(matches!(
            session.on_bind(
                "expired-portal".to_string(),
                "insert".to_string(),
                &[],
                &[Some(b"expired-write".to_vec())],
                vec![],
                &crate::jsonb_wire::test_limits()
            ),
            BackendMessage::BindComplete
        ));

        let messages = execute_portal_inner(&ctx, &mut session, "expired-portal").await;
        assert!(
            is_error(&messages, "40001"),
            "expired extended DML must fail before it is acknowledged: {messages:?}"
        );
        assert!(session.in_failed_txn());
        assert_eq!(session.txn_writes().len(), 0);

        execute_simple(&ctx, &mut session, "ROLLBACK").await;
        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn serializable_transaction_reads_its_own_buffered_insert() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        execute_simple(&ctx, &mut session, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        execute_simple(
            &ctx,
            &mut session,
            "INSERT INTO kv (k, v) VALUES ('own-write', 'visible')",
        )
        .await;

        let result =
            execute_simple(&ctx, &mut session, "SELECT v FROM kv WHERE k = 'own-write'").await;
        let value = result.iter().find_map(|message| match message {
            BackendMessage::DataRow { columns } => columns
                .first()
                .and_then(Option::as_ref)
                .and_then(|bytes| String::from_utf8(bytes.clone()).ok()),
            _ => None,
        });
        assert_eq!(value.as_deref(), Some("visible"));
        execute_simple(&ctx, &mut session, "ROLLBACK").await;
        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn transaction_reads_its_own_update_and_delete_but_rollback_hides_them() {
        let (_dir, ctx) = make_ctx().await;
        let mut seed = Session::new(superuser());
        execute_simple(
            &ctx,
            &mut seed,
            "INSERT INTO kv (k, v) VALUES ('own-update', 'old')",
        )
        .await;
        let mut writer = Session::new(superuser());
        let mut observer = Session::new(superuser());
        execute_simple(&ctx, &mut writer, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        execute_simple(
            &ctx,
            &mut writer,
            "UPDATE kv SET v = 'new' WHERE k = 'own-update'",
        )
        .await;
        let updated =
            execute_simple(&ctx, &mut writer, "SELECT v FROM kv WHERE k = 'own-update'").await;
        assert_eq!(read_first_text_column(&updated).as_deref(), Some("new"));

        execute_simple(&ctx, &mut writer, "DELETE FROM kv WHERE k = 'own-update'").await;
        let deleted =
            execute_simple(&ctx, &mut writer, "SELECT v FROM kv WHERE k = 'own-update'").await;
        assert_eq!(read_first_text_column(&deleted), None);
        assert_eq!(
            row_count(&ctx, "own-update").await,
            1,
            "uncommitted writes stay invisible to other sessions"
        );

        execute_simple(&ctx, &mut writer, "ROLLBACK").await;
        let after_rollback = execute_simple(
            &ctx,
            &mut observer,
            "SELECT v FROM kv WHERE k = 'own-update'",
        )
        .await;
        assert_eq!(
            read_first_text_column(&after_rollback).as_deref(),
            Some("old")
        );
        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn serializable_read_write_skew_is_rejected_at_commit() {
        let (_dir, ctx) = make_ctx().await;
        let mut seed = Session::new(superuser());
        execute_simple(
            &ctx,
            &mut seed,
            "INSERT INTO kv (k, v) VALUES ('left', '0')",
        )
        .await;
        execute_simple(
            &ctx,
            &mut seed,
            "INSERT INTO kv (k, v) VALUES ('right', '0')",
        )
        .await;

        let mut first = Session::new(superuser());
        let mut second = Session::new(superuser());
        execute_simple(&ctx, &mut first, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        execute_simple(&ctx, &mut second, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        execute_simple(&ctx, &mut first, "SELECT v FROM kv WHERE k = 'right'").await;
        execute_simple(&ctx, &mut second, "SELECT v FROM kv WHERE k = 'left'").await;
        execute_simple(&ctx, &mut first, "UPDATE kv SET v = '1' WHERE k = 'left'").await;
        execute_simple(&ctx, &mut second, "UPDATE kv SET v = '1' WHERE k = 'right'").await;

        let first_commit = execute_simple(&ctx, &mut first, "COMMIT").await;
        assert!(
            matches!(&first_commit[..], [BackendMessage::CommandComplete { tag }] if tag == "COMMIT")
        );
        let second_commit = execute_simple(&ctx, &mut second, "COMMIT").await;
        assert!(
            is_error(&second_commit, "40001"),
            "stale serializable write must abort: {second_commit:?}"
        );
        let unchanged =
            execute_simple(&ctx, &mut first, "SELECT v FROM kv WHERE k = 'right'").await;
        assert_eq!(read_first_text_column(&unchanged).as_deref(), Some("0"));

        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn serializable_predicate_read_detects_a_phantom_insert() {
        let (_dir, ctx) = make_ctx().await;
        let mut seed = Session::new(superuser());
        execute_simple(
            &ctx,
            &mut seed,
            "INSERT INTO kv (k, v) VALUES ('anchor', '0')",
        )
        .await;

        let mut reader = Session::new(superuser());
        let mut inserter = Session::new(superuser());
        execute_simple(&ctx, &mut reader, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        execute_simple(&ctx, &mut reader, "SELECT v FROM kv WHERE k = 'missing'").await;
        execute_simple(&ctx, &mut inserter, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        execute_simple(
            &ctx,
            &mut inserter,
            "INSERT INTO kv (k, v) VALUES ('missing', 'phantom')",
        )
        .await;
        assert!(
            matches!(&execute_simple(&ctx, &mut inserter, "COMMIT").await[..], [BackendMessage::CommandComplete { tag }] if tag == "COMMIT")
        );

        execute_simple(
            &ctx,
            &mut reader,
            "UPDATE kv SET v = '1' WHERE k = 'anchor'",
        )
        .await;
        let commit = execute_simple(&ctx, &mut reader, "COMMIT").await;
        assert!(
            is_error(&commit, "40001"),
            "predicate phantom must abort: {commit:?}"
        );
        ctx.engine.shutdown().unwrap();
    }

    #[tokio::test]
    async fn extended_protocol_select_uses_serializable_snapshot() {
        let (_dir, ctx) = make_ctx().await;
        let mut writer = Session::new(superuser());
        execute_simple(
            &ctx,
            &mut writer,
            "INSERT INTO kv (k, v) VALUES ('extended-snapshot', 'before')",
        )
        .await;

        let mut reader = Session::new(superuser());
        execute_simple(&ctx, &mut reader, "BEGIN ISOLATION LEVEL SERIALIZABLE").await;
        reader.on_parse(
            "read".to_string(),
            "SELECT v FROM kv WHERE k = 'extended-snapshot'",
            vec![],
        );
        reader.on_bind(
            "portal".to_string(),
            "read".to_string(),
            &[],
            &[],
            vec![],
            &crate::jsonb_wire::test_limits(),
        );
        let first = execute_portal(&ctx, &mut reader, "portal").await;
        assert_eq!(read_first_text_column(&first).as_deref(), Some("before"));

        execute_simple(
            &ctx,
            &mut writer,
            "UPDATE kv SET v = 'after' WHERE k = 'extended-snapshot'",
        )
        .await;
        // A portal run to its end returns no more rows (PostgreSQL), so read
        // again through a fresh bind of the same statement.
        reader.on_bind(
            "portal".to_string(),
            "read".to_string(),
            &[],
            &[],
            vec![],
            &crate::jsonb_wire::test_limits(),
        );
        let second = execute_portal(&ctx, &mut reader, "portal").await;
        assert_eq!(read_first_text_column(&second).as_deref(), Some("before"));
        execute_simple(&ctx, &mut reader, "ROLLBACK").await;
        ctx.engine.shutdown().unwrap();
    }

    // ---- `::` cast: pgbench's object-existence check (`$1::pg_catalog.regclass`) ----

    /// pgbench -i's object-existence check, the exact statement and the exact
    /// protocol (Parse → Bind → Execute) the server runs:
    ///
    /// ```text
    /// SELECT relkind FROM pg_catalog.pg_class WHERE oid=$1::pg_catalog.regclass
    /// ```
    ///
    /// with `$1` bound to a real relation name. Before `::` existed the lexer
    /// refused the `:` with `bad token: :`. The cast must now resolve the name to
    /// the relation's real `pg_class.oid` — the SAME OID `pg_class` projects — so the
    /// `oid =` comparison matches and the query returns that relation's actual
    /// `relkind` ('r' for an ordinary table), not merely "no error".
    #[tokio::test]
    async fn the_pgbench_object_existence_check_returns_the_real_relkind() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        assert!(matches!(
            session.on_parse(
                "exists".to_string(),
                "SELECT relkind FROM pg_catalog.pg_class WHERE oid=$1::pg_catalog.regclass",
                vec![],
            ),
            BackendMessage::ParseComplete
        ));
        assert!(matches!(
            session.on_bind(
                "p".to_string(),
                "exists".to_string(),
                &[],
                &[Some(b"kv".to_vec())],
                vec![],
                &crate::jsonb_wire::test_limits(),
            ),
            BackendMessage::BindComplete
        ));

        let messages = execute_portal(&ctx, &mut session, "p").await;
        assert!(
            !messages
                .iter()
                .any(|m| matches!(m, BackendMessage::ErrorResponse { .. })),
            "the object-existence check must not error: {messages:?}"
        );
        assert_eq!(
            read_first_text_column(&messages).as_deref(),
            Some("r"),
            "the cast resolved `kv` to its real oid, so pg_class returned its relkind"
        );
        ctx.engine.shutdown().unwrap();
    }

    /// Negative control for the case above: an unresolvable relation name is an
    /// ERROR (PostgreSQL `42P01`), never an empty result that a client would read
    /// as "the object does not exist".
    #[tokio::test]
    async fn the_pgbench_object_existence_check_errors_on_an_unknown_relation() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        session.on_parse(
            "exists".to_string(),
            "SELECT relkind FROM pg_catalog.pg_class WHERE oid=$1::pg_catalog.regclass",
            vec![],
        );
        session.on_bind(
            "p".to_string(),
            "exists".to_string(),
            &[],
            &[Some(b"no_such_relation".to_vec())],
            vec![],
            &crate::jsonb_wire::test_limits(),
        );

        let messages = execute_portal(&ctx, &mut session, "p").await;
        assert!(
            is_error(&messages, "42P01"),
            "an unresolvable relation name must be undefined_table: {messages:?}"
        );
        assert!(
            !messages
                .iter()
                .any(|m| matches!(m, BackendMessage::DataRow { .. })),
            "and must not be an empty result that looks like success: {messages:?}"
        );
        ctx.engine.shutdown().unwrap();
    }

    /// The `pg_catalog.pg_class` projection is served to the query path (the cast
    /// test above depends on it): a plain simple query reads a relation's relkind
    /// with no cast involved.
    #[tokio::test]
    async fn pg_catalog_pg_class_is_queryable_and_reports_relkind() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        let messages = execute_simple(
            &ctx,
            &mut session,
            "SELECT relkind FROM pg_catalog.pg_class WHERE relname = 'kv'",
        )
        .await;
        assert_eq!(read_first_text_column(&messages).as_deref(), Some("r"));
        ctx.engine.shutdown().unwrap();
    }

    /// A select-list `::regclass` resolves the same way the WHERE one does:
    /// `'kv'::regclass` yields the relation's `pg_class.oid` (as an integer).
    #[tokio::test]
    async fn a_scalar_regclass_cast_yields_the_relations_oid() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        let messages = execute_simple(&ctx, &mut session, "SELECT 'kv'::regclass").await;
        let expected = crate::catalog::resolve_regclass(&ctx.schema, "public", "kv")
            .expect("kv resolves to its oid")
            .to_string();
        assert_eq!(
            read_first_text_column(&messages).as_deref(),
            Some(expected.as_str()),
            "the scalar cast must resolve to the real oid, not be dropped"
        );
        ctx.engine.shutdown().unwrap();
    }

    /// A cast to a type ferrosa does not implement is refused BY NAME at parse
    /// time (never accepted and ignored): the type as written is in the message.
    #[tokio::test]
    async fn a_cast_to_an_unsupported_type_is_refused_by_name() {
        let (_dir, ctx) = make_ctx().await;
        let mut session = Session::new(superuser());
        let response = session.on_parse(
            "bad".to_string(),
            "SELECT relkind FROM pg_catalog.pg_class WHERE oid = $1::text",
            vec![],
        );
        match response {
            BackendMessage::ErrorResponse { fields } => {
                assert_eq!(fields[1], (b'C', "0A000".to_string()));
                assert!(
                    fields[2].1.contains('`') && fields[2].1.contains("text"),
                    "the refusal names the type: {fields:?}"
                );
            }
            other => panic!("expected ErrorResponse, got {other:?}"),
        }
        ctx.engine.shutdown().unwrap();
    }

    fn read_first_text_column(messages: &[BackendMessage]) -> Option<String> {
        messages.iter().find_map(|message| match message {
            BackendMessage::DataRow { columns } => columns
                .first()
                .and_then(Option::as_ref)
                .and_then(|bytes| String::from_utf8(bytes.clone()).ok()),
            _ => None,
        })
    }

    /// Frame one frontend message exactly as a client would: a tag byte, a big-endian length
    /// that includes the length word itself, then the body.
    fn pg_frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + body.len());
        out.push(tag);
        out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    /// A simple-query (`Q`) message carrying one NUL-terminated SQL string.
    fn pg_query(sql: &str) -> Vec<u8> {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        pg_frame(b'Q', &body)
    }

    /// A `CopyData` (`d`) frame.
    fn pg_copy_data(data: &[u8]) -> Vec<u8> {
        pg_frame(b'd', data)
    }

    /// Drive a whole client byte stream through the REAL connection loop (`query_loop`) and
    /// return everything the server wrote back, as text.
    ///
    /// This is the seam the `COPY` tests in `copy_stdin.rs` do NOT cross: they call
    /// `copy_stdin::drive` directly. Only `query_loop` holds the fast-path gate that decides
    /// whether a `COPY` statement is handed to `drive` at all, so only a test that runs through
    /// here can prove the handshake actually happens.
    async fn run_wire(ctx: &QueryContext, wire: &[u8]) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut client, mut server) = tokio::io::duplex(1 << 20);
        // The pipe must hold the whole request plus the server's reply without a reader running.
        client.write_all(wire).await.unwrap();

        let mut frames = bytes::BytesMut::new();
        let mut read_buf = vec![0u8; 1 << 16];
        let auth = superuser();
        query_loop(&mut server, &mut frames, ctx, auth, &mut read_buf)
            .await
            .expect("query_loop returns on Terminate or EOF");
        // Close our end so the read below reaches EOF instead of blocking forever.
        drop(server);

        let mut reply = Vec::new();
        let mut buf = vec![0u8; 1 << 16];
        loop {
            match tokio::time::timeout(std::time::Duration::from_millis(500), client.read(&mut buf))
                .await
            {
                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                Ok(Ok(n)) => reply.extend_from_slice(&buf[..n]),
            }
        }
        String::from_utf8_lossy(&reply).into_owned()
    }

    /// Count the `CopyInResponse` frames in a server reply — the 8-byte ack `G`, length `7`,
    /// format `0`, column count `0`. Counting the FRAME rather than the letter `G` is deliberate:
    /// a payload line or a command tag may legitimately contain a `G`.
    ///
    /// The ack is the one and only cue for the client to start streaming, so a second one — a
    /// stale copy carried into the tail write — re-cues a real client into copy mode.
    fn copy_in_responses(text: &str) -> usize {
        const FRAME: [u8; 8] = [b'G', 0, 0, 0, 7, 0, 0, 0];
        text.as_bytes()
            .windows(FRAME.len())
            .filter(|window| *window == FRAME)
            .count()
    }

    /// `pgbench -i` (client-side data generation) speaks the LEGACY libpq copy protocol: it sends
    /// the copy rows, then the in-band end-of-data marker `\.` as a `CopyData` line, and only then
    /// calls `PQendcopy`, which sends a `CopyDone`. A server that does not honor `\.` decodes it as
    /// a bad payload (a lone `\` before `.` is an unknown escape) and the whole load fails.
    ///
    /// This drives the exact byte stream through `query_loop` and asserts the rows LAND.
    #[tokio::test]
    async fn pgbench_legacy_copy_end_marker_lands_rows() {
        let (_dir, ctx) = make_ctx().await;
        let mut wire = Vec::new();
        wire.extend(pg_query("copy kv from stdin with (freeze on)"));
        wire.extend(pg_copy_data(b"a\tb\n"));
        // pgbench's `PQputline(con, "\\.\n")`.
        wire.extend(pg_copy_data(b"\\.\n"));
        // `PQendcopy` sends `CopyDone`.
        wire.extend(pg_frame(b'c', &[]));
        wire.extend(pg_frame(b'X', &[])); // Terminate

        let text = run_wire(&ctx, &wire).await;

        assert!(
            text.starts_with('G'),
            "the COPY must be acknowledged with CopyInResponse first: {text:?}"
        );
        assert!(
            text.contains("COPY 1"),
            "the row before the `\\.` marker must be reported as loaded: {text:?}"
        );
        assert!(
            !text.contains("22P04"),
            "the `\\.` marker must not be decoded as a payload error: {text:?}"
        );
        assert!(
            !text.contains("08P01"),
            "the trailing CopyDone from PQendcopy must not be a protocol violation: {text:?}"
        );
        assert_eq!(
            copy_in_responses(&text),
            1,
            "pgbench's legacy COPY is acknowledged exactly once: {text:?}"
        );
        assert_eq!(
            row_count(&ctx, "a").await,
            1,
            "the row must have landed, not merely been acknowledged"
        );
    }

    /// The fast-path gate in `query_loop` and the ordinary parser must agree on what a COPY
    /// statement is. psql appends the statement terminator, so the wire text carries a trailing
    /// `;`; the gate must still route it to `copy_stdin::drive`.
    #[tokio::test]
    async fn a_copy_statement_with_a_trailing_semicolon_still_enters_copy_mode() {
        let (_dir, ctx) = make_ctx().await;
        let mut wire = Vec::new();
        wire.extend(pg_query("copy kv from stdin;"));
        wire.extend(pg_copy_data(b"z\tq\n"));
        wire.extend(pg_frame(b'c', &[]));
        wire.extend(pg_frame(b'X', &[]));

        let text = run_wire(&ctx, &wire).await;

        assert!(
            text.starts_with('G'),
            "a trailing `;` must still open COPY mode: {text:?}"
        );
        assert!(text.contains("COPY 1"), "the row is counted: {text:?}");
        assert_eq!(row_count(&ctx, "z").await, 1, "the row must have landed");
    }

    /// `pgbench -i` wraps its whole data load in `begin` → 3× `COPY` → `commit`. The
    /// SAME `COPY ... FROM STDIN` that works in autocommit must therefore also enter
    /// COPY mode when a transaction block is open. The gate that decides this lives in
    /// `query_loop` (the fast-path `COPY` check), NOT in `copy_stdin::drive`, so this
    /// test drives the whole `BEGIN`/`Query(copy)`/`CopyData`/`CopyDone`/`COMMIT` byte
    /// stream through `query_loop` — the seam the existing transactional COPY tests
    /// (`run_copy_in`) bypass by calling `drive()` directly. That bypass is why a green
    /// suite coexisted with a live `PQendcopy failed`.
    ///
    /// RED first: before the fix, the COPY's `Query` never reaches `drive`, the client's
    /// payload arrives as stray frames, and the server answers `08P01` instead of
    /// `CopyInResponse`. Asserting the rows LAND (read back) is the point: an
    /// acknowledgement that drops the payload would also "not error".
    #[tokio::test]
    async fn copy_inside_a_transaction_over_the_wire_enters_copy_mode_and_lands_rows() {
        let (_dir, ctx) = make_ctx().await;
        let mut wire = Vec::new();
        wire.extend(pg_query("BEGIN"));
        wire.extend(pg_query("copy kv from stdin"));
        wire.extend(pg_copy_data(b"a\tb\n"));
        wire.extend(pg_frame(b'c', &[])); // CopyDone
        wire.extend(pg_query("COMMIT"));
        wire.extend(pg_frame(b'X', &[])); // Terminate

        let text = run_wire(&ctx, &wire).await;

        assert!(
            text.contains('G'),
            "a COPY inside a transaction must still be acknowledged with \
             CopyInResponse: {text:?}"
        );
        assert!(
            !text.contains("08P01"),
            "the payload must not be rejected as stray frames outside a COPY: {text:?}"
        );
        assert!(
            text.contains("COPY 1"),
            "the COPY still reports its own row count: {text:?}"
        );
        assert_eq!(
            copy_in_responses(&text),
            1,
            "the COPY is acknowledged exactly once: {text:?}"
        );
        assert_eq!(
            row_count(&ctx, "a").await,
            1,
            "the row must land after COMMIT — the whole point of the fix"
        );
    }

    /// The ACID half, through the same `query_loop` seam: a COPY inside a transaction
    /// whose `ROLLBACK` follows must leave NO rows. Without this, "it entered COPY mode"
    /// could still mean a COPY that writes regardless of the block's outcome.
    #[tokio::test]
    async fn copy_inside_a_rolled_back_transaction_over_the_wire_leaves_no_rows() {
        let (_dir, ctx) = make_ctx().await;
        let mut wire = Vec::new();
        wire.extend(pg_query("BEGIN"));
        wire.extend(pg_query("copy kv from stdin"));
        wire.extend(pg_copy_data(b"a\tb\n"));
        wire.extend(pg_frame(b'c', &[]));
        wire.extend(pg_query("ROLLBACK"));
        wire.extend(pg_frame(b'X', &[]));

        let text = run_wire(&ctx, &wire).await;

        assert!(
            text.contains("COPY 1"),
            "the COPY itself succeeds inside the block: {text:?}"
        );
        assert!(
            !text.contains("08P01"),
            "no stray-frame rejection: {text:?}"
        );
        assert_eq!(
            copy_in_responses(&text),
            1,
            "the COPY is acknowledged exactly once: {text:?}"
        );
        assert_eq!(
            row_count(&ctx, "a").await,
            0,
            "ROLLBACK must discard the buffered COPY rows"
        );
    }

    /// The legacy `\.` end-of-data marker must keep working when the COPY runs inside a
    /// transaction too — `pgbench -i` sends it in exactly that shape.
    #[tokio::test]
    async fn copy_inside_a_transaction_honors_the_legacy_marker() {
        let (_dir, ctx) = make_ctx().await;
        let mut wire = Vec::new();
        wire.extend(pg_query("BEGIN"));
        wire.extend(pg_query("copy kv from stdin with (freeze on)"));
        wire.extend(pg_copy_data(b"a\tb\n"));
        wire.extend(pg_copy_data(b"\\.\n"));
        wire.extend(pg_frame(b'c', &[]));
        wire.extend(pg_query("COMMIT"));
        wire.extend(pg_frame(b'X', &[]));

        let text = run_wire(&ctx, &wire).await;

        assert!(
            text.contains('G'),
            "COPY mode must open inside the transaction: {text:?}"
        );
        assert!(
            !text.contains("22P04"),
            "the `\\.` marker must not be decoded as a payload error: {text:?}"
        );
        assert!(text.contains("COPY 1"), "the row is counted: {text:?}");
        assert_eq!(
            copy_in_responses(&text),
            1,
            "the COPY is acknowledged exactly once, marker and all: {text:?}"
        );
        assert_eq!(
            row_count(&ctx, "a").await,
            1,
            "the row before the marker must land on COMMIT"
        );
    }
}
