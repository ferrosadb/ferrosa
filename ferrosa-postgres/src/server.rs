//! Tokio TCP front-end that drives the sans-IO [`Connection`] over a socket.
//!
//! This is the thin I/O wrapper: read bytes → `Connection::on_bytes` → write
//! bytes. All protocol logic lives in the sans-IO layers, so this module stays
//! small and is exercised end-to-end by a real driver in the integration tests.

use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
use bytes::BytesMut;
use rand::RngCore;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::connection::{ConnError, Connection};
use crate::handshake::{HandshakeError, VerifierStore};
use crate::messages::BackendMessage;

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

/// Drive one connection to completion over `stream`: returns on clean close, EOF,
/// or after sending a fatal ErrorResponse.
pub async fn handle_connection<St, S>(mut stream: St, store: Arc<S>) -> std::io::Result<()>
where
    St: AsyncRead + AsyncWrite + Unpin,
    S: VerifierStore,
{
    let mut conn = Connection::new(&*store, random_server_nonce());
    let mut buf = [0u8; 8192];
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break; // client closed the socket
        }
        match conn.on_bytes(&buf[..n]) {
            Ok(out) => {
                if !out.is_empty() {
                    stream.write_all(&out).await?;
                }
                if conn.is_closed() {
                    break;
                }
            }
            Err(e) => {
                let mut eb = BytesMut::new();
                BackendMessage::ErrorResponse {
                    fields: vec![
                        (b'S', "FATAL".to_string()),
                        (b'C', sqlstate(&e).to_string()),
                        (b'M', format!("{e:?}")),
                    ],
                }
                .encode(&mut eb);
                let _ = stream.write_all(&eb).await;
                break;
            }
        }
    }
    Ok(())
}

/// Accept loop: serve Postgres connections from `listener`, one spawned task per
/// connection, until the listener errors.
pub async fn serve<S>(listener: TcpListener, store: Arc<S>) -> std::io::Result<()>
where
    S: VerifierStore + Send + Sync + 'static,
{
    loop {
        let (stream, _peer) = listener.accept().await?;
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let _ = handle_connection(stream, store).await;
        });
    }
}
