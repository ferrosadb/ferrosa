//! Parse `FERROSA_CQL_BROADCAST` into the `(ip, port)` pair advertised to
//! CQL drivers via `system.local.rpc_address` and `system.local.rpc_port`.
//!
//! Port-mapped container clusters must advertise a host-reachable port
//! (e.g. `19042`), not the container-internal bind port (`9042`), or
//! drivers like cdrs-tokio hang during session bootstrap trying to
//! re-dial the local node using the advertised address. See
//! `specs/in-process/bug-cql-auth-enabled-cluster-times-out-for-cdrs-clients.md`.
//!
//! An unusable value is an error, never a silent `127.0.0.1`: advertising
//! loopback tells every driver to connect to itself, which looks like a healthy
//! cluster to the operator and a dead one to every client.

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};

/// Resolve `FERROSA_CQL_BROADCAST` into the IP + port that should be
/// advertised via `system.local`. `fallback_port` is used when the env
/// var contains only an IP or host (no `:port` suffix).
///
/// Accepts:
/// - `"127.0.0.1:19042"` → `(127.0.0.1, 19042)`
/// - `"127.0.0.1"` → `(127.0.0.1, fallback_port)`
/// - `"host.containers.internal:19043"` → DNS-resolved IP + 19043
/// - `"host.containers.internal"` → DNS-resolved IP + fallback_port
///
/// Returns an error naming the value when it is empty or does not parse or resolve.
pub fn parse_cql_broadcast(raw: &str, fallback_port: u16) -> Result<(IpAddr, u16), String> {
    let raw = raw.trim();
    let unusable = |why: &str| {
        format!("FERROSA_CQL_BROADCAST={raw:?} is not usable: {why}; use ip[:port] or host[:port]")
    };
    if raw.is_empty() {
        return Err(unusable("it is empty"));
    }
    if let Ok(sa) = raw.parse::<SocketAddr>() {
        return Ok((sa.ip(), sa.port()));
    }
    if let Ok(ip) = raw.parse::<IpAddr>() {
        return Ok((ip, fallback_port));
    }
    // DNS path: split `host[:port]`, resolve host, use provided port or fallback.
    let (host, port) = match raw.rsplit_once(':') {
        Some((h, p)) => match p.parse::<u16>() {
            Ok(n) => (h, n),
            Err(_) => (raw, fallback_port),
        },
        None => (raw, fallback_port),
    };
    match format!("{host}:{port}").to_socket_addrs() {
        Ok(mut addrs) => addrs
            .next()
            .map(|sa| (sa.ip(), port))
            .ok_or_else(|| unusable("the host resolved to no address")),
        Err(e) => Err(unusable(&format!("could not resolve {host:?}: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn ip_port_pair_parses_both() {
        let (ip, port) = parse_cql_broadcast("127.0.0.1:19042", 9042).unwrap();
        assert_eq!(ip, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(port, 19042, "port from env var must win over fallback");
    }

    #[test]
    fn ipv4_alone_uses_fallback_port() {
        let (ip, port) = parse_cql_broadcast("127.0.0.1", 9042).unwrap();
        assert_eq!(ip, IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(port, 9042);
    }

    #[test]
    fn ipv6_with_port_parses() {
        let (ip, port) = parse_cql_broadcast("[::1]:19042", 9042).unwrap();
        assert!(ip.is_loopback());
        assert_eq!(port, 19042);
    }

    #[test]
    fn a_resolvable_host_gets_its_port() {
        // `localhost` resolves everywhere without network access.
        let (ip, port) = parse_cql_broadcast("localhost:19043", 9042).unwrap();
        assert!(ip.is_loopback());
        assert_eq!(port, 19043);
        let (_, port) = parse_cql_broadcast("localhost", 9042).unwrap();
        assert_eq!(port, 9042);
    }

    /// The old behavior returned `(127.0.0.1, fallback_port)` for all of these, so a
    /// typo advertised loopback to every driver with no message.
    #[test]
    fn an_unusable_value_is_an_error_that_names_it() {
        for bad in ["", "   ", "not a host name", "bad host:19042", "10.0.0.999"] {
            let err = parse_cql_broadcast(bad, 9042).expect_err(bad);
            assert!(
                err.contains("FERROSA_CQL_BROADCAST"),
                "{bad:?}: the error names the variable: {err}"
            );
        }
        let err = parse_cql_broadcast("bad host:19042", 9042).unwrap_err();
        assert!(
            err.contains("bad host:19042"),
            "and quotes the value: {err}"
        );
    }

    /// This is the specific scenario behind
    /// bug-cql-auth-enabled-cluster-times-out-for-cdrs-clients.md:
    /// inside the container, the CQL server binds to `0.0.0.0:9042`,
    /// but the host-reachable mapped port is `19042`. The env var is
    /// `"127.0.0.1:19042"`. `rpc_port` MUST be 19042, NOT 9042.
    #[test]
    fn port_mapped_container_advertises_host_port_not_bind_port() {
        let bind_port = 9042; // what the container binds to
        let (_ip, port) = parse_cql_broadcast("127.0.0.1:19042", bind_port).unwrap();
        assert_eq!(
            port, 19042,
            "advertised port MUST be the host-reachable port; \
             returning bind_port ({bind_port}) would cause cdrs-tokio \
             to dial an unreachable address during session bootstrap"
        );
    }
}
