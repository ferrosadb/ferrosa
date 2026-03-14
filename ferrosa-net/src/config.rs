use std::net::SocketAddr;
use std::time::Duration;

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
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:7000".parse().unwrap(),
            broadcast_addr: "127.0.0.1:7000".parse().unwrap(),
            seeds: Vec::new(),
            cluster_name: "ferrosa".to_string(),
            psk: None,
            heartbeat_interval: Duration::from_millis(500),
            heartbeat_timeout: Duration::from_millis(1500),
            max_connections: 100,
            handshake_timeout: Duration::from_secs(5),
            max_frame_body_size: 256 * 1024 * 1024, // 256 MiB
            max_streams_per_lane: 128,
        }
    }
}

impl NetConfig {
    /// Build config from environment variables, with defaults.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        if let Ok(v) = std::env::var("FERROSA_INTERNODE_BIND") {
            if let Ok(addr) = v.parse() {
                cfg.bind_addr = addr;
            }
        }
        if let Ok(v) = std::env::var("FERROSA_INTERNODE_BROADCAST") {
            if let Ok(addr) = v.parse() {
                cfg.broadcast_addr = addr;
            }
        }
        if let Ok(v) = std::env::var("FERROSA_SEED") {
            cfg.seeds = v.split(',').filter_map(|s| s.trim().parse().ok()).collect();
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

        cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let cfg = NetConfig::default();
        assert_eq!(cfg.bind_addr, "0.0.0.0:7000".parse().unwrap());
        assert_eq!(cfg.cluster_name, "ferrosa");
        assert!(cfg.psk.is_none());
        assert_eq!(cfg.max_connections, 100);
        assert_eq!(cfg.max_frame_body_size, 256 * 1024 * 1024);
        assert_eq!(cfg.max_streams_per_lane, 128);
        assert_eq!(cfg.heartbeat_interval, Duration::from_millis(500));
        assert_eq!(cfg.heartbeat_timeout, Duration::from_millis(1500));
        assert_eq!(cfg.handshake_timeout, Duration::from_secs(5));
    }
}
