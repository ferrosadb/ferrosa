use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use crate::codec::Lane;

/// Configuration for the ferrosa-net transport layer.
/// All values can be overridden via environment variables.
#[derive(Debug, Clone)]
pub struct NetConfig {
    /// Address to bind the internode listener.
    pub bind_addr: SocketAddr,
    /// Address advertised to peers (defaults to bind_addr).
    pub broadcast_addr: SocketAddr,
    /// Seed addresses for bootstrap (from --seed CLI or FERROSA_SEED env).
    pub seeds: Vec<SocketAddr>,
    /// Cluster name — must match across all nodes.
    pub cluster_name: String,
    /// Pre-shared key for handshake authentication (Phase 1).
    pub psk: Option<String>,
    /// Heartbeat ping interval.
    pub heartbeat_interval: Duration,
    /// Peer suspected-dead after this duration without heartbeat.
    pub heartbeat_timeout: Duration,
    /// Max inbound internode connections (T5 mitigation).
    pub max_connections: usize,
    /// Max time to complete handshake before closing connection (T5).
    pub handshake_timeout: Duration,
    /// Max frame body size in bytes (T3 mitigation).
    pub max_frame_body_size: u32,
    /// Max concurrent streams per connection lane (T15).
    pub max_streams_per_lane: usize,
    /// Default timeout for Raft-lane RPCs.
    pub raft_lane_timeout: Duration,
    /// Default timeout for Data-lane RPCs.
    pub data_lane_timeout: Duration,
    /// Default timeout for Bulk-lane RPCs.
    pub bulk_lane_timeout: Duration,
    /// Process-wide cap for concurrently dispatched Data-lane RPCs.
    pub data_lane_max_in_flight: usize,
    /// Path to TLS certificate file (PEM) for internode encryption.
    pub tls_cert_path: Option<String>,
    /// Path to TLS private key file (PEM).
    pub tls_key_path: Option<String>,
    /// Path to CA certificate file (PEM) for mutual TLS verification.
    pub tls_ca_path: Option<String>,
    /// If true, reject startup when no TLS cert/key are configured.
    pub require_tls: bool,
    /// CQL broadcast address advertised to peers during handshake.
    /// Peers use this for `system.peers.native_address`.
    pub cql_broadcast: Option<String>,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            // Port 17000 instead of the historical Cassandra default 7000 —
            // 7000 is reserved by macOS ControlCenter and produces an opaque
            // EADDRINUSE crash on every fresh macOS install (BUG-001).
            bind_addr: "0.0.0.0:17000".parse().unwrap(),
            broadcast_addr: "127.0.0.1:17000".parse().unwrap(),
            seeds: Vec::new(),
            cluster_name: "ferrosa".to_string(),
            psk: None,
            heartbeat_interval: Duration::from_millis(500),
            heartbeat_timeout: Duration::from_millis(1500),
            max_connections: 512,
            handshake_timeout: Duration::from_secs(5),
            max_frame_body_size: 256 * 1024 * 1024, // 256 MiB
            max_streams_per_lane: 128,
            raft_lane_timeout: Lane::Raft.timeout(),
            data_lane_timeout: Lane::Data.timeout(),
            bulk_lane_timeout: Lane::Bulk.timeout(),
            data_lane_max_in_flight: 256,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            require_tls: false,
            cql_broadcast: None,
        }
    }
}

impl NetConfig {
    fn parse_socket_addr(raw: &str) -> Option<SocketAddr> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }
        if let Ok(addr) = trimmed.parse() {
            return Some(addr);
        }
        let mut resolved = trimmed.to_socket_addrs().ok()?;
        resolved.next()
    }

    fn parse_seed_list(raw: &str) -> Vec<SocketAddr> {
        raw.split(',').filter_map(Self::parse_socket_addr).collect()
    }

    fn parse_duration_ms_env(name: &str) -> Option<Duration> {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&ms| ms > 0)
            .map(Duration::from_millis)
    }

    pub fn lane_timeout(&self, lane: Lane) -> Duration {
        match lane {
            Lane::Raft => self.raft_lane_timeout,
            Lane::Data => self.data_lane_timeout,
            Lane::Bulk => self.bulk_lane_timeout,
        }
    }

    /// Build config from environment variables, with defaults.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        if let Ok(v) = std::env::var("FERROSA_INTERNODE_BIND") {
            if let Ok(addr) = v.parse() {
                cfg.bind_addr = addr;
            }
        }
        if let Ok(v) = std::env::var("FERROSA_INTERNODE_BROADCAST") {
            if let Some(addr) = Self::parse_socket_addr(&v) {
                cfg.broadcast_addr = addr;
            }
        }
        if let Ok(v) = std::env::var("FERROSA_SEED") {
            cfg.seeds = Self::parse_seed_list(&v);
        }
        if let Ok(v) = std::env::var("FERROSA_CLUSTER_NAME") {
            cfg.cluster_name = v;
        }
        if let Ok(v) = std::env::var("FERROSA_INTERNODE_PSK") {
            cfg.psk = Some(v);
        }
        if let Ok(v) = std::env::var("FERROSA_HEARTBEAT_INTERVAL_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                cfg.heartbeat_interval = Duration::from_millis(ms);
            }
        }
        if let Ok(v) = std::env::var("FERROSA_HEARTBEAT_TIMEOUT_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                cfg.heartbeat_timeout = Duration::from_millis(ms);
            }
        }
        if let Ok(v) = std::env::var("FERROSA_MAX_INTERNODE_CONNECTIONS") {
            if let Ok(n) = v.parse() {
                cfg.max_connections = n;
            }
        }
        if let Ok(v) = std::env::var("FERROSA_HANDSHAKE_TIMEOUT_SECS") {
            if let Ok(s) = v.parse::<u64>() {
                cfg.handshake_timeout = Duration::from_secs(s);
            }
        }
        if let Ok(v) = std::env::var("FERROSA_MAX_FRAME_BODY_SIZE") {
            if let Ok(n) = v.parse() {
                cfg.max_frame_body_size = n;
            }
        }
        if let Ok(v) = std::env::var("FERROSA_MAX_STREAMS_PER_LANE") {
            if let Ok(n) = v.parse() {
                cfg.max_streams_per_lane = n;
            }
        }
        if let Some(timeout) = Self::parse_duration_ms_env("FERROSA_RAFT_ELECTION_MIN_MS") {
            cfg.raft_lane_timeout = timeout / 3;
        }
        if let Some(timeout) = Self::parse_duration_ms_env("FERROSA_RAFT_LANE_TIMEOUT_MS") {
            cfg.raft_lane_timeout = timeout;
        }
        if let Some(timeout) = Self::parse_duration_ms_env("FERROSA_DATA_LANE_TIMEOUT_MS") {
            cfg.data_lane_timeout = timeout;
        }
        if let Some(timeout) = Self::parse_duration_ms_env("FERROSA_BULK_LANE_TIMEOUT_MS") {
            cfg.bulk_lane_timeout = timeout;
        }
        if let Ok(v) = std::env::var("FERROSA_DATA_LANE_MAX_IN_FLIGHT") {
            if let Ok(n) = v.parse::<usize>() {
                if n > 0 {
                    cfg.data_lane_max_in_flight = n;
                }
            }
        }
        if let Ok(v) = std::env::var("FERROSA_INTERNODE_TLS_CERT") {
            cfg.tls_cert_path = Some(v);
        }
        if let Ok(v) = std::env::var("FERROSA_INTERNODE_TLS_KEY") {
            cfg.tls_key_path = Some(v);
        }
        if let Ok(v) = std::env::var("FERROSA_INTERNODE_TLS_CA") {
            cfg.tls_ca_path = Some(v);
        }
        if let Ok(v) = std::env::var("FERROSA_INTERNODE_REQUIRE_TLS") {
            cfg.require_tls = v == "true" || v == "1";
        }
        if let Ok(v) = std::env::var("FERROSA_CQL_BROADCAST") {
            cfg.cql_broadcast = Some(v);
        }

        cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// BUG-001: the default internode port must NOT be 7000 on any platform
    /// — that port is reserved by macOS ControlCenter and produces an
    /// opaque EADDRINUSE crash loop on every fresh macOS install. 17000
    /// is high enough to avoid OS-reserved port ranges and uncommon
    /// enough to dodge most popular services.
    #[test]
    fn default_bind_port_is_not_7000() {
        let cfg = NetConfig::default();
        assert_ne!(
            cfg.bind_addr.port(),
            7000,
            "port 7000 conflicts with macOS ControlCenter — see BUG-001"
        );
    }

    #[test]
    fn default_config_values() {
        let cfg = NetConfig::default();
        assert_eq!(cfg.bind_addr, "0.0.0.0:17000".parse().unwrap());
        assert_eq!(cfg.cluster_name, "ferrosa");
        assert!(cfg.psk.is_none());
        assert_eq!(cfg.max_connections, 512);
        assert_eq!(cfg.max_frame_body_size, 256 * 1024 * 1024);
        assert_eq!(cfg.max_streams_per_lane, 128);
        assert_eq!(cfg.raft_lane_timeout, Duration::from_secs(1));
        assert_eq!(cfg.data_lane_timeout, Duration::from_secs(10));
        assert_eq!(cfg.bulk_lane_timeout, Duration::from_secs(60));
        assert_eq!(cfg.data_lane_max_in_flight, 256);
        assert_eq!(cfg.heartbeat_interval, Duration::from_millis(500));
        assert_eq!(cfg.heartbeat_timeout, Duration::from_millis(1500));
        assert_eq!(cfg.handshake_timeout, Duration::from_secs(5));
    }

    #[test]
    fn parse_socket_addr_accepts_hostname_entries() {
        let addr =
            NetConfig::parse_socket_addr("localhost:7000").expect("localhost should resolve");
        assert!(addr.ip().is_loopback());
        assert_eq!(addr.port(), 7000);
    }

    #[test]
    fn parse_seed_list_accepts_hostname_entries() {
        let seeds = NetConfig::parse_seed_list("localhost:7000,127.0.0.1:7001");
        assert_eq!(seeds.len(), 2);
        assert!(seeds[0].ip().is_loopback());
        assert_eq!(seeds[0].port(), 7000);
        assert_eq!(seeds[1], "127.0.0.1:7001".parse().unwrap());
    }
}
