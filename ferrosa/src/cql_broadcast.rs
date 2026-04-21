//! Parse `FERROSA_CQL_BROADCAST` into the `(ip, port)` pair advertised to
//! CQL drivers via `system.local.rpc_address` and `system.local.rpc_port`.
//!
//! Port-mapped container clusters must advertise a host-reachable port
//! (e.g. `19042`), not the container-internal bind port (`9042`), or
//! drivers like cdrs-tokio hang during session bootstrap trying to
//! re-dial the local node using the advertised address. See
//! `specs/in-process/bug-cql-auth-enabled-cluster-times-out-for-cdrs-clients.md`.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};

/// Resolve `FERROSA_CQL_BROADCAST` into the IP + port that should be
/// advertised via `system.local`. `fallback_port` is used when the env
/// var contains only an IP (no `:port` suffix).
///
/// Accepts:
/// - `"127.0.0.1:19042"` → `(127.0.0.1, 19042)`
/// - `"127.0.0.1"` → `(127.0.0.1, fallback_port)`
/// - `"host.containers.internal:19043"` → DNS-resolved IP + 19043
/// - `"host.containers.internal"` → DNS-resolved IP + fallback_port
///
/// Returns `(127.0.0.1, fallback_port)` if nothing parses.
pub fn parse_cql_broadcast(raw: &str, fallback_port: u16) -> (IpAddr, u16) {
    if let Ok(sa) = raw.parse::<SocketAddr>() {
        return (sa.ip(), sa.port());
    }
    if let Ok(ip) = raw.parse::<IpAddr>() {
        return (ip, fallback_port);
    }
    // DNS path: split `host[:port]`, resolve host, use provided port or fallback.
    let (host, port) = match raw.rsplit_once(':') {
        Some((h, p)) => match p.parse::<u16>() {
            Ok(n) => (h, n),
            Err(_) => (raw, fallback_port),
        },
        None => (raw, fallback_port),
    };
    if let Ok(mut addrs) = format!("{host}:{port}").to_socket_addrs() {
        if let Some(sa) = addrs.next() {
            return (sa.ip(), port);
        }
    }
    (IpAddr::V4(Ipv4Addr::LOCALHOST), fallback_port)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ip_port_pair_parses_both() {
        let (ip, port) = parse_cql_broadcast("127.0.0.1:19042", 9042);
        assert_eq!(ip, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(port, 19042, "port from env var must win over fallback");
    }

    #[test]
    fn ipv4_alone_uses_fallback_port() {
        let (ip, port) = parse_cql_broadcast("127.0.0.1", 9042);
        assert_eq!(ip, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(port, 9042);
    }

    #[test]
    fn ipv6_with_port_parses() {
        let (ip, port) = parse_cql_broadcast("[::1]:19042", 9042);
        assert!(ip.is_loopback());
        assert_eq!(port, 19042);
    }

    #[test]
    fn empty_or_garbage_falls_back_to_loopback() {
        let (ip, port) = parse_cql_broadcast("", 9042);
        assert_eq!(ip, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(port, 9042);
    }

    /// This is the specific scenario behind
    /// bug-cql-auth-enabled-cluster-times-out-for-cdrs-clients.md:
    /// inside the container, the CQL server binds to `0.0.0.0:9042`,
    /// but the host-reachable mapped port is `19042`. The env var is
    /// `"127.0.0.1:19042"`. `rpc_port` MUST be 19042, NOT 9042.
    #[test]
    fn port_mapped_container_advertises_host_port_not_bind_port() {
        let bind_port = 9042; // what the container binds to
        let (_ip, port) = parse_cql_broadcast("127.0.0.1:19042", bind_port);
        assert_eq!(
            port, 19042,
            "advertised port MUST be the host-reachable port; \
             returning bind_port ({bind_port}) would cause cdrs-tokio \
             to dial an unreachable address during session bootstrap"
        );
    }
}
