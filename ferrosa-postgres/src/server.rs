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
use crate::handshake::{HandshakeError, VerifierStore};
use crate::messages::{BackendMessage, FrontendMessage, TransactionStatus};
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

/// Frame and serve simple queries until the client terminates or the socket
/// closes. `frames` is seeded with any already-buffered post-auth bytes.
async fn query_loop<St>(
    stream: &mut St,
    frames: &mut BytesMut,
    ctx: &QueryContext,
    read_buf: &mut [u8],
) -> std::io::Result<()>
where
    St: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        match codec::read_frontend(frames) {
            Ok(Some(FrontendMessage::Query(sql))) => {
                let msgs =
                    query::execute_query(&ctx.engine, &ctx.schema, &sql, &ctx.default_schema).await;
                let mut out = BytesMut::new();
                for m in &msgs {
                    m.encode(&mut out);
                }
                BackendMessage::ReadyForQuery(TransactionStatus::Idle).encode(&mut out);
                stream.write_all(&out).await?;
            }
            Ok(Some(FrontendMessage::Sync)) => {
                let mut out = BytesMut::new();
                BackendMessage::ReadyForQuery(TransactionStatus::Idle).encode(&mut out);
                stream.write_all(&out).await?;
            }
            Ok(Some(FrontendMessage::Terminate)) => return Ok(()),
            // Unknown / not-yet-handled post-auth messages: ignore (extended-query
            // Parse/Bind/etc. are a later slice), continue framing.
            Ok(Some(_)) => {}
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
