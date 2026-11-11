//! An in-memory network for tests that run on tokio's paused clock
//! (t_68b0a1df).
//!
//! A paused clock auto-advances to the next timer whenever the runtime has no
//! runnable task, and a loopback socket waiting on the kernel is not a
//! runnable task. Under CPU load a real connect or handshake therefore loses
//! to its own virtual timeout. That made the slow-retry lane tests fail about
//! one run in twenty under load while the lane code was correct.
//!
//! Addresses in TEST-NET-2 (198.51.100.0/24, reserved for documentation, so no
//! real peer uses it) are served from this module instead of the kernel:
//! [`RpcClient::connect_with_tls_on_pool`](crate::rpc::client::RpcClient)
//! dials them through [`connect`]. Bytes cross a `tokio::io::duplex` pipe, so
//! every wake is in-process and the clock can only advance once both ends are
//! genuinely waiting. Every other address takes the real TCP path.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::OnceLock;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use tokio::io::DuplexStream;
use tokio::sync::mpsc;

const NET: [u8; 3] = [198, 51, 100];
/// Pending connections a listener holds before further dials are refused,
/// like a full TCP backlog.
const BACKLOG: usize = 64;
/// Bytes buffered in each direction of a connection.
const PIPE_BYTES: usize = 64 * 1024;

type Registry = DashMap<SocketAddr, mpsc::Sender<DuplexStream>>;

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(DashMap::new)
}

/// Whether `addr` belongs to the in-memory network.
pub(crate) fn is_mem(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(ip) => ip.octets()[..3] == NET,
        IpAddr::V6(_) => false,
    }
}

/// A fresh in-memory address that nothing has bound. Unique for the life of
/// the process, so tests running in parallel never share one.
pub(crate) fn free_addr() -> SocketAddr {
    static NEXT_PORT: AtomicU16 = AtomicU16::new(1);
    let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
    assert_ne!(port, 0, "in-memory port space exhausted");
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(NET[0], NET[1], NET[2], 1)), port)
}

/// Dial `addr`. `None` means `addr` is not in-memory and the caller must use
/// the real network. Nothing bound, or a full backlog, is `ConnectionRefused`.
pub(crate) fn connect(addr: SocketAddr) -> Option<io::Result<DuplexStream>> {
    if !is_mem(addr) {
        return None;
    }
    let refused = || io::Error::new(io::ErrorKind::ConnectionRefused, format!("mem {addr}"));
    let Some(sender) = registry().get(&addr).map(|s| s.clone()) else {
        return Some(Err(refused()));
    };
    let (client, server) = tokio::io::duplex(PIPE_BYTES);
    Some(match sender.try_send(server) {
        Ok(()) => Ok(client),
        Err(_) => Err(refused()),
    })
}

/// A bound in-memory listener. Dropping it unbinds the address and refuses
/// connections still in its backlog, as closing a TCP listener does.
pub(crate) struct MemListener {
    addr: SocketAddr,
    incoming: mpsc::Receiver<DuplexStream>,
}

impl MemListener {
    /// Bind `addr`, which must be in-memory. A second bind of a bound address
    /// fails with `AddrInUse`.
    pub(crate) fn bind(addr: SocketAddr) -> io::Result<Self> {
        assert!(is_mem(addr), "{addr} is not an in-memory address");
        let (tx, incoming) = mpsc::channel(BACKLOG);
        match registry().entry(addr) {
            Entry::Occupied(_) => Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("mem {addr}"),
            )),
            Entry::Vacant(slot) => {
                slot.insert(tx);
                Ok(Self { addr, incoming })
            }
        }
    }

    /// The next connection. Never returns `None` while `self` is alive,
    /// because the registry holds the sender.
    pub(crate) async fn accept(&mut self) -> DuplexStream {
        self.incoming
            .recv()
            .await
            .expect("registry holds the sender while the listener is bound")
    }

    /// A connection already waiting, if any. Lets a test prove that nothing
    /// dialed this address.
    pub(crate) fn try_accept(&mut self) -> Option<DuplexStream> {
        self.incoming.try_recv().ok()
    }
}

impl Drop for MemListener {
    fn drop(&mut self) {
        let removed = registry().remove(&self.addr);
        debug_assert!(
            removed.is_some(),
            "mem listener {} was not registered",
            self.addr
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn a_dial_reaches_the_listener_bound_at_that_address() {
        let addr = free_addr();
        let mut listener = MemListener::bind(addr).expect("bind");
        let mut client = connect(addr).expect("in-memory").expect("connect");
        let mut server = listener.accept().await;
        client.write_all(b"ping").await.expect("write");
        let mut buf = [0u8; 4];
        server.read_exact(&mut buf).await.expect("read");
        assert_eq!(&buf, b"ping");
    }

    #[test]
    fn a_dial_with_nothing_bound_is_refused() {
        let err = connect(free_addr())
            .expect("in-memory")
            .expect_err("refused");
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
    }

    #[test]
    fn dropping_the_listener_unbinds_the_address() {
        let addr = free_addr();
        let listener = MemListener::bind(addr).expect("bind");
        assert_eq!(
            MemListener::bind(addr).err().map(|e| e.kind()),
            Some(io::ErrorKind::AddrInUse)
        );
        drop(listener);
        let err = connect(addr).expect("in-memory").expect_err("unbound");
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
        MemListener::bind(addr).expect("rebind after drop");
    }

    #[test]
    fn a_real_address_is_left_to_the_real_network() {
        assert!(connect("127.0.0.1:9".parse().expect("addr")).is_none());
        assert!(connect("192.0.2.1:9".parse().expect("addr")).is_none());
    }
}
