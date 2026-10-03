//! A query whose client stopped reading its socket holds no blocking thread.
//!
//! No portal is suspended here: the client sends a query and then never reads
//! the reply, so TCP backpressure stops the server mid-result. The result
//! stream's socket write waits (async), its executor holds no thread between
//! fetches, and the storage scan beneath it pauses and returns its thread.
//!
//! Its own test binary: the premise check reads a process-wide counter.

#[path = "common/blocking_pool.rs"]
mod blocking_pool;
#[path = "common/pg_server.rs"]
mod pg_server;

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use blocking_pool::{eventually, on_bounded_runtime, settle_blocking_pool, MAX_BLOCKING, SETTLE};
use pg_server::{connect, start_server};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Config, NoTls};

/// Wide enough that `SELECT *` overflows the client's and server's socket
/// buffers many times over, so the server is genuinely stopped mid-result.
const ROWS: usize = 30_000;

/// A client transport that stops reading once `stalled` is set, the way a
/// client that stopped draining its socket does. Writes still go through, so
/// the query reaches the server; its replies pile up in the socket buffers.
struct StallingStream {
    inner: TcpStream,
    stalled: Arc<AtomicBool>,
}

impl AsyncRead for StallingStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.stalled.load(Ordering::SeqCst) {
            // Never woken: the test never resumes reading.
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for StallingStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// A client that has authenticated and now reads nothing. The simple query
/// protocol is one message, so a query sent on it reaches the server without
/// the client reading anything first.
async fn stalled_client(port: u16) -> (tokio_postgres::Client, Arc<AtomicBool>) {
    let tcp = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    let stalled = Arc::new(AtomicBool::new(false));
    let stream = StallingStream {
        inner: tcp,
        stalled: Arc::clone(&stalled),
    };
    let (client, connection) = Config::new()
        .user("ferrosa_user")
        .password("devpass")
        .dbname("ferrosa")
        .ssl_mode(SslMode::Disable)
        .connect_raw(stream, NoTls)
        .await
        .expect("SCRAM handshake succeeds");
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("stalled driver connection ended: {error}");
        }
    });
    stalled.store(true, Ordering::SeqCst);
    (client, stalled)
}

#[test]
fn clients_that_stop_reading_hold_no_blocking_thread() {
    on_bounded_runtime(async {
        ferrosa_sched::init_global_pool(ferrosa_sched::Reservation::new(17, 1));
        let server = start_server(ROWS).await;
        let releases = ferrosa_sched::scan_releases_total();

        let mut stalled = Vec::new();
        for _ in 0..2 * MAX_BLOCKING {
            let (client, flag) = stalled_client(server.port).await;
            let query = tokio::spawn(async move { client.simple_query("SELECT * FROM t").await });
            stalled.push((query, flag));
        }

        // The premise: each stalled client's storage scan really stopped
        // mid-table and paused, because nobody drained it.
        let paused = eventually(SETTLE, || {
            ferrosa_sched::scan_releases_total() >= releases + stalled.len() as u64
        })
        .await;
        assert!(
            paused,
            "only {} of {} stalled clients' scans paused",
            ferrosa_sched::scan_releases_total() - releases,
            stalled.len()
        );
        for (query, _) in &stalled {
            assert!(!query.is_finished(), "a stalled client's query completed");
        }

        let free = settle_blocking_pool().await;
        assert_eq!(
            free,
            MAX_BLOCKING,
            "{} clients that stopped reading still hold {} of {MAX_BLOCKING} blocking \
             threads after {SETTLE:?}",
            stalled.len(),
            MAX_BLOCKING - free
        );

        let other = connect(server.port).await;
        let rows = tokio::time::timeout(
            Duration::from_secs(20),
            other.query("SELECT id FROM t", &[]),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "another session's SELECT made no progress in 20 s beside {} stalled clients",
                stalled.len()
            )
        })
        .expect("the other session's SELECT succeeds");
        assert_eq!(rows.len(), ROWS, "every row is returned");
        for (query, _) in stalled {
            query.abort();
        }
    });
}
