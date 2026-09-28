//! Flight gRPC server bootstrap.
//!
//! `flight_service` wraps [`FerrosaFlight`] in the generated
//! `FlightServiceServer` so callers can mount it on a `tonic` server (with
//! their own shutdown / incoming wiring); [`serve_service`] is the
//! bind-and-run path used by the binary, over TLS when a certificate is
//! configured (t_58db6320).
//!
//! # TLS
//!
//! TLS is terminated here with `tokio-rustls`, not with tonic's own `tls`
//! feature: the `rustls::ServerConfig` comes from `ferrosa_net::tls`, the one
//! place the process chooses its crypto provider, so Flight cannot drift onto
//! a second provider. Each accepted TCP connection completes its TLS handshake
//! in its own task (bounded by [`MAX_PENDING_HANDSHAKES`] and
//! [`TLS_HANDSHAKE_TIMEOUT`]) and only then is handed to tonic through
//! `serve_with_incoming`. ALPN is `h2` only ([`ferrosa_net::tls::GRPC_ALPN`]).
//!
//! With a certificate configured the port speaks TLS only: a plaintext gRPC
//! client's HTTP/2 preface is not a TLS ClientHello, the handshake fails and
//! the connection is closed (logged at WARN with the peer address).

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use arrow_flight::flight_service_server::FlightServiceServer;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tonic::transport::server::{Connected, TcpConnectInfo};

use ferrosa_cql::router::SharedState;

use crate::service::FerrosaFlight;

/// Listener name used in TLS configuration errors and logs.
pub const LISTENER_NAME: &str = "Arrow Flight";

/// Longest a client may take to complete the TLS handshake.
pub const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Upper bound on TLS handshakes in flight at once. When reached, the accept
/// loop waits for one to finish before accepting another connection
/// (backpressure instead of unbounded task growth).
pub const MAX_PENDING_HANDSHAKES: usize = 256;

/// Pause after a failed `accept()` (e.g. EMFILE) so a persistent failure does
/// not spin the accept loop.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// TLS settings for the Flight listener (`[flight] tls_cert` / `tls_key` /
/// `require_tls`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlightTlsConfig {
    /// PEM certificate chain path.
    pub cert_path: Option<String>,
    /// PEM private key path.
    pub key_path: Option<String>,
    /// Refuse to start without a certificate.
    pub require_tls: bool,
}

impl FlightTlsConfig {
    /// Build the rustls server config through `ferrosa_net::tls` (the shared
    /// crypto provider), advertising ALPN `h2`.
    ///
    /// `Ok(None)` means plaintext (no certificate, not required).
    /// `require_tls` without a certificate, or only one of cert/key, is an
    /// error naming the listener — never a plaintext fallback.
    pub fn server_config(&self) -> Result<Option<Arc<rustls::ServerConfig>>, ServeError> {
        ferrosa_net::tls::optional_server_config(
            LISTENER_NAME,
            self.cert_path.as_deref(),
            self.key_path.as_deref(),
            self.require_tls,
            ferrosa_net::tls::GRPC_ALPN,
        )
        .map_err(|e| ServeError::Tls(e.to_string()))
    }
}

/// Why the Flight server could not start or stopped.
#[derive(Debug)]
pub enum ServeError {
    /// TLS configuration could not be built (names the listener and file).
    Tls(String),
    /// Binding the listen address failed.
    Bind {
        /// The address that could not be bound.
        addr: SocketAddr,
        /// The underlying error.
        source: io::Error,
    },
    /// The tonic transport failed.
    Transport(tonic::transport::Error),
}

impl std::fmt::Display for ServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tls(e) => write!(f, "{LISTENER_NAME} TLS: {e}"),
            Self::Bind { addr, source } => {
                write!(f, "{LISTENER_NAME}: cannot bind {addr}: {source}")
            }
            Self::Transport(e) => write!(f, "{LISTENER_NAME} transport: {e}"),
        }
    }
}

impl std::error::Error for ServeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Tls(_) => None,
            Self::Bind { source, .. } => Some(source),
            Self::Transport(e) => Some(e),
        }
    }
}

/// Wrap the Flight service in its gRPC server adapter, ready to add to a
/// `tonic::transport::Server` (or serve over a custom incoming stream in tests).
pub fn flight_service(
    state: Arc<SharedState>,
    signing_key: Vec<u8>,
) -> FlightServiceServer<FerrosaFlight> {
    FlightServiceServer::new(FerrosaFlight::new(state, signing_key))
}

/// Bind `addr` and serve a pre-configured Flight service (e.g. with key
/// rotation / custom TTL), over TLS when `tls` has a certificate.
///
/// The TLS configuration is built before binding, so a bad certificate path
/// or `require_tls` without a certificate fails without opening the port.
pub async fn serve_service(
    addr: SocketAddr,
    service: FerrosaFlight,
    tls: &FlightTlsConfig,
) -> Result<(), ServeError> {
    let server_config = tls.server_config()?;
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|source| ServeError::Bind { addr, source })?;
    serve_on_listener(listener, FlightServiceServer::new(service), server_config).await
}

/// Serve `service` on an already-bound listener: TLS-only when
/// `server_config` is `Some`, plaintext gRPC otherwise.
pub async fn serve_on_listener(
    listener: TcpListener,
    service: FlightServiceServer<FerrosaFlight>,
    server_config: Option<Arc<rustls::ServerConfig>>,
) -> Result<(), ServeError> {
    let local = listener.local_addr().ok();
    let router = tonic::transport::Server::builder().add_service(service);
    match server_config {
        Some(config) => {
            tracing::info!(addr = ?local, "Arrow Flight server listening (TLS)");
            router
                .serve_with_incoming(tls_incoming(listener, config))
                .await
        }
        None => {
            tracing::info!(addr = ?local, "Arrow Flight server listening (plaintext)");
            router.serve_with_incoming(plain_incoming(listener)).await
        }
    }
    .map_err(ServeError::Transport)
}

/// Serve the Flight endpoint on `addr` in plaintext until the future is
/// dropped/cancelled. Test and embedding convenience; the binary uses
/// [`serve_service`] with its `[flight]` TLS settings.
///
/// Auth is enforced per-RPC (see [`crate::service`]); the endpoint is only
/// anonymous-safe because every read RPC requires a verified bearer token.
pub async fn serve(
    addr: SocketAddr,
    state: Arc<SharedState>,
    signing_key: Vec<u8>,
) -> Result<(), ServeError> {
    serve_service(
        addr,
        FerrosaFlight::new(state, signing_key),
        &FlightTlsConfig::default(),
    )
    .await
}

/// A server-side TLS connection handed to tonic. Carries the TCP connect info
/// so `Request::remote_addr()` keeps working behind TLS.
pub struct TlsConnection {
    stream: tokio_rustls::server::TlsStream<TcpStream>,
    info: TcpConnectInfo,
}

impl Connected for TlsConnection {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> TcpConnectInfo {
        self.info.clone()
    }
}

impl AsyncRead for TlsConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
}

/// Plaintext incoming stream: accepted TCP connections, accept errors logged
/// on the failing/recovered edges and retried after a short backoff.
fn plain_incoming(
    listener: TcpListener,
) -> impl futures::Stream<Item = Result<TcpStream, io::Error>> {
    futures::stream::unfold(
        (listener, AcceptEdges::default()),
        |(listener, mut edges)| async move {
            let stream = accept_with_backoff(&listener, &mut edges).await.0;
            Some((Ok(stream), (listener, edges)))
        },
    )
}

/// TLS incoming stream. A background task accepts TCP connections and runs
/// each handshake in its own task; completed handshakes are yielded to tonic.
/// The task ends when tonic drops the stream (the receiver closes).
fn tls_incoming(
    listener: TcpListener,
    config: Arc<rustls::ServerConfig>,
) -> impl futures::Stream<Item = Result<TlsConnection, io::Error>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<TlsConnection>(MAX_PENDING_HANDSHAKES);
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    tokio::spawn(accept_tls_loop(listener, acceptor, tx));
    futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|conn| (Ok(conn), rx))
    })
}

async fn accept_tls_loop(
    listener: TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
    tx: tokio::sync::mpsc::Sender<TlsConnection>,
) {
    let permits = Arc::new(tokio::sync::Semaphore::new(MAX_PENDING_HANDSHAKES));
    let mut edges = AcceptEdges::default();
    // Server accept loop: runs until the server drops the incoming stream.
    while !tx.is_closed() {
        let permit = match Arc::clone(&permits).acquire_owned().await {
            Ok(permit) => permit,
            Err(closed) => {
                tracing::error!(error = %closed, "Arrow Flight handshake semaphore closed");
                return;
            }
        };
        let (tcp, peer) = accept_with_backoff(&listener, &mut edges).await;
        let info = tcp.connect_info();
        let acceptor = acceptor.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                Ok(Ok(stream)) => {
                    if tx.send(TlsConnection { stream, info }).await.is_err() {
                        tracing::debug!(%peer, "Arrow Flight server stopped before connection was served");
                    }
                }
                Ok(Err(e)) => tracing::warn!(
                    %peer, error = %e,
                    "Arrow Flight TLS handshake failed; connection closed (plaintext client?)"
                ),
                Err(_) => tracing::warn!(
                    %peer, timeout = ?TLS_HANDSHAKE_TIMEOUT,
                    "Arrow Flight TLS handshake timed out; connection closed"
                ),
            }
        });
    }
}

/// Tracks whether `accept()` is currently failing so an outage is reported as
/// two lines (started failing / recovered), not one per attempt.
#[derive(Default)]
struct AcceptEdges {
    failing: bool,
}

async fn accept_with_backoff(
    listener: &TcpListener,
    edges: &mut AcceptEdges,
) -> (TcpStream, SocketAddr) {
    loop {
        match listener.accept().await {
            Ok(accepted) => {
                if edges.failing {
                    tracing::info!("Arrow Flight accept() recovered");
                    edges.failing = false;
                }
                return accepted;
            }
            Err(e) => {
                if !edges.failing {
                    tracing::error!(error = %e, "Arrow Flight accept() failing; retrying");
                    edges.failing = true;
                }
                tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
            }
        }
    }
}
