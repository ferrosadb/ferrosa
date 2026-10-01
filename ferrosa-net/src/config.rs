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
    /// Bound on DNS resolution and on the TCP connect, each, for every outbound
    /// dial (fast reconnect, slow-retry probe, peer re-dial). Without it a peer
    /// that blackholes SYN stretches probe cadence to the OS connect timeout.
    /// Together with `handshake_timeout` a dial is bounded by the sum of the three.
    pub connect_timeout: Duration,
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
    /// Raw, unresolved internode broadcast target (hostname:port) advertised to
    /// peers during the handshake. Unlike [`Self::broadcast_addr`], this preserves
    /// the configured `FERROSA_INTERNODE_BROADCAST` hostname so peers store the
    /// hostname (not a startup-frozen IP) in `NodeInfo.addr` and re-resolve it on
    /// every reconnect — handling container IP churn without stale membership.
    pub internode_broadcast: Option<String>,
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
            connect_timeout: Duration::from_secs(5),
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
            internode_broadcast: None,
        }
    }
}

/// One problem found while reading the internode configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigIssue {
    /// The environment variable.
    pub var: &'static str,
    /// The value that was rejected.
    pub value: String,
    /// Why it was rejected.
    pub reason: String,
    /// `true`: an operator typo the node must not start with. `false`: a condition
    /// that may clear by itself (a hostname whose DNS is not ready yet), reported
    /// but not fatal.
    pub fatal: bool,
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

    /// The internode address this node advertises to peers for `NodeInfo.addr`.
    ///
    /// Prefers the raw, re-resolvable [`Self::internode_broadcast`] hostname when
    /// configured; otherwise falls back to the resolved [`Self::broadcast_addr`].
    /// Storing the hostname lets peers re-resolve it on every reconnect, so a
    /// container IP change is handled without stale committed membership.
    pub fn advertised_internode_addr(&self) -> String {
        self.internode_broadcast
            .clone()
            .unwrap_or_else(|| self.broadcast_addr.to_string())
    }

    pub fn lane_timeout(&self, lane: Lane) -> Duration {
        match lane {
            Lane::Raft => self.raft_lane_timeout,
            Lane::Data => self.data_lane_timeout,
            Lane::Bulk => self.bulk_lane_timeout,
        }
    }
}

/// Reads variables through `lookup`, collecting an issue for every value it cannot
/// use instead of dropping it. An empty value counts as unset (compose files write
/// `VAR=`).
struct Reader<'a> {
    lookup: &'a dyn Fn(&str) -> Option<String>,
    issues: Vec<ConfigIssue>,
}

impl Reader<'_> {
    fn raw(&self, var: &str) -> Option<String> {
        (self.lookup)(var).filter(|value| !value.trim().is_empty())
    }

    fn issue(&mut self, var: &'static str, value: &str, reason: String, fatal: bool) {
        self.issues.push(ConfigIssue {
            var,
            value: value.to_string(),
            reason,
            fatal,
        });
    }

    fn parsed<T>(&mut self, var: &'static str) -> Option<T>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        let raw = self.raw(var)?;
        match raw.trim().parse::<T>() {
            Ok(value) => Some(value),
            Err(e) => {
                self.issue(var, &raw, format!("not a valid value: {e}"), true);
                None
            }
        }
    }

    fn positive_ms(&mut self, var: &'static str) -> Option<Duration> {
        let raw = self.raw(var)?;
        let millis: u64 = self.parsed(var)?;
        if millis == 0 {
            self.issue(var, &raw, "must be greater than zero".into(), true);
            return None;
        }
        Some(Duration::from_millis(millis))
    }

    fn boolean(&mut self, var: &'static str) -> Option<bool> {
        let raw = self.raw(var)?;
        match raw.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => {
                self.issue(var, &raw, "expected true or false".into(), true);
                None
            }
        }
    }
}

impl NetConfig {
    /// Read the configuration from `lookup` (the environment in production), returning
    /// the best-effort config AND every problem found. Nothing is dropped silently.
    pub fn from_lookup(lookup: &dyn Fn(&str) -> Option<String>) -> (Self, Vec<ConfigIssue>) {
        let mut cfg = Self::default();
        let mut r = Reader {
            lookup,
            issues: Vec::new(),
        };

        if let Some(addr) = r.parsed("FERROSA_INTERNODE_BIND") {
            cfg.bind_addr = addr;
        }
        if let Some(v) = r.raw("FERROSA_INTERNODE_BROADCAST") {
            match Self::parse_socket_addr(&v) {
                Some(addr) => cfg.broadcast_addr = addr,
                // Often DNS that is not ready yet: keep the raw name for peers to
                // re-resolve (below) and report it.
                None => r.issue(
                    "FERROSA_INTERNODE_BROADCAST",
                    &v,
                    "could not be parsed or resolved; advertising the raw name to peers".into(),
                    false,
                ),
            }
            // Preserve the RAW (unresolved) hostname:port so it can be advertised
            // to peers and re-resolved on every reconnect. We keep it even for IP
            // literals — harmless, since re-resolving an IP is a no-op.
            cfg.internode_broadcast = Some(v.trim().to_string());
        }
        if let Some(v) = r.raw("FERROSA_SEED") {
            for entry in v.split(',').map(str::trim).filter(|e| !e.is_empty()) {
                match Self::parse_socket_addr(entry) {
                    Some(addr) => cfg.seeds.push(addr),
                    None => r.issue(
                        "FERROSA_SEED",
                        entry,
                        "could not be parsed or resolved; left out of the seed list".into(),
                        false,
                    ),
                }
            }
        }
        if let Some(v) = r.raw("FERROSA_CLUSTER_NAME") {
            cfg.cluster_name = v;
        }
        if let Some(v) = r.raw("FERROSA_INTERNODE_PSK") {
            cfg.psk = Some(v);
        }
        if let Some(ms) = r.parsed::<u64>("FERROSA_HEARTBEAT_INTERVAL_MS") {
            cfg.heartbeat_interval = Duration::from_millis(ms);
        }
        if let Some(ms) = r.parsed::<u64>("FERROSA_HEARTBEAT_TIMEOUT_MS") {
            cfg.heartbeat_timeout = Duration::from_millis(ms);
        }
        if let Some(n) = r.parsed("FERROSA_MAX_INTERNODE_CONNECTIONS") {
            cfg.max_connections = n;
        }
        if let Some(s) = r.parsed::<u64>("FERROSA_HANDSHAKE_TIMEOUT_SECS") {
            cfg.handshake_timeout = Duration::from_secs(s);
        }
        if let Some(timeout) = r.positive_ms("FERROSA_CONNECT_TIMEOUT_MS") {
            cfg.connect_timeout = timeout;
        }
        if let Some(n) = r.parsed("FERROSA_MAX_FRAME_BODY_SIZE") {
            cfg.max_frame_body_size = n;
        }
        if let Some(n) = r.parsed("FERROSA_MAX_STREAMS_PER_LANE") {
            cfg.max_streams_per_lane = n;
        }
        if let Some(timeout) = r.positive_ms("FERROSA_RAFT_ELECTION_MIN_MS") {
            cfg.raft_lane_timeout = timeout / 3;
        }
        if let Some(timeout) = r.positive_ms("FERROSA_RAFT_LANE_TIMEOUT_MS") {
            cfg.raft_lane_timeout = timeout;
        }
        if let Some(timeout) = r.positive_ms("FERROSA_DATA_LANE_TIMEOUT_MS") {
            cfg.data_lane_timeout = timeout;
        }
        if let Some(timeout) = r.positive_ms("FERROSA_BULK_LANE_TIMEOUT_MS") {
            cfg.bulk_lane_timeout = timeout;
        }
        if let Some(raw) = r.raw("FERROSA_DATA_LANE_MAX_IN_FLIGHT") {
            match r.parsed::<usize>("FERROSA_DATA_LANE_MAX_IN_FLIGHT") {
                Some(0) => r.issue(
                    "FERROSA_DATA_LANE_MAX_IN_FLIGHT",
                    &raw,
                    "must be greater than zero".into(),
                    true,
                ),
                Some(n) => cfg.data_lane_max_in_flight = n,
                None => {}
            }
        }
        if let Some(v) = r.raw("FERROSA_INTERNODE_TLS_CERT") {
            cfg.tls_cert_path = Some(v);
        }
        if let Some(v) = r.raw("FERROSA_INTERNODE_TLS_KEY") {
            cfg.tls_key_path = Some(v);
        }
        if let Some(v) = r.raw("FERROSA_INTERNODE_TLS_CA") {
            cfg.tls_ca_path = Some(v);
        }
        // Any value other than a boolean used to read as `false`, so a typo such as
        // `ture` silently turned the TLS requirement off.
        if let Some(required) = r.boolean("FERROSA_INTERNODE_REQUIRE_TLS") {
            cfg.require_tls = required;
        }
        if let Some(v) = r.raw("FERROSA_CQL_BROADCAST") {
            cfg.cql_broadcast = Some(v);
        }

        (cfg, r.issues)
    }

    /// Like [`Self::from_lookup`], but a fatal issue is an error listing all of them.
    pub fn checked_from_lookup(lookup: &dyn Fn(&str) -> Option<String>) -> Result<Self, String> {
        let (cfg, issues) = Self::from_lookup(lookup);
        Self::fatal_issues_as_error(&issues).map_or(Ok(cfg), Err)
    }

    fn fatal_issues_as_error(issues: &[ConfigIssue]) -> Option<String> {
        let fatal: Vec<String> = issues
            .iter()
            .filter(|issue| issue.fatal)
            .map(|issue| format!("{}={:?}: {}", issue.var, issue.value, issue.reason))
            .collect();
        (!fatal.is_empty())
            .then(|| format!("invalid internode configuration: {}", fatal.join("; ")))
    }

    fn log_issues(issues: &[ConfigIssue]) {
        for issue in issues {
            if issue.fatal {
                tracing::error!(var = issue.var, value = %issue.value, reason = %issue.reason,
                    "invalid internode configuration value ignored");
            } else {
                tracing::warn!(var = issue.var, value = %issue.value, reason = %issue.reason,
                    "internode configuration value not usable yet");
            }
        }
    }

    /// Build config from environment variables, with defaults. Every rejected value
    /// is logged (ERROR for a typo, WARN for a name that may resolve later) and the
    /// default is kept; use [`Self::from_env_checked`] to refuse to start on a typo.
    pub fn from_env() -> Self {
        let (cfg, issues) = Self::from_lookup(&|name| std::env::var(name).ok());
        Self::log_issues(&issues);
        cfg
    }

    /// Like [`Self::from_env`], but a typo in a value is an error instead of a logged
    /// default. Values that may resolve later (seed and broadcast hostnames) are
    /// logged at WARN and do not stop startup.
    pub fn from_env_checked() -> Result<Self, String> {
        let (cfg, issues) = Self::from_lookup(&|name| std::env::var(name).ok());
        Self::log_issues(&issues);
        Self::fatal_issues_as_error(&issues).map_or(Ok(cfg), Err)
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
        assert_eq!(cfg.connect_timeout, Duration::from_secs(5));
    }

    fn env(pairs: &[(&'static str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: std::collections::HashMap<&'static str, String> =
            pairs.iter().map(|(k, v)| (*k, v.to_string())).collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn valid_values_are_read_with_no_issues() {
        let (cfg, issues) = NetConfig::from_lookup(&env(&[
            ("FERROSA_INTERNODE_BIND", "0.0.0.0:17001"),
            ("FERROSA_SEED", "127.0.0.1:17000,localhost:17002"),
            ("FERROSA_CLUSTER_NAME", "lab"),
            ("FERROSA_HEARTBEAT_INTERVAL_MS", "250"),
            ("FERROSA_MAX_INTERNODE_CONNECTIONS", "64"),
            ("FERROSA_INTERNODE_REQUIRE_TLS", "true"),
        ]));
        assert!(issues.is_empty(), "{issues:?}");
        assert_eq!(cfg.bind_addr, "0.0.0.0:17001".parse().unwrap());
        assert_eq!(cfg.seeds.len(), 2);
        assert_eq!(cfg.cluster_name, "lab");
        assert_eq!(cfg.heartbeat_interval, Duration::from_millis(250));
        assert_eq!(cfg.max_connections, 64);
        assert!(cfg.require_tls);
    }

    #[test]
    fn unset_and_empty_values_are_not_problems() {
        let (cfg, issues) = NetConfig::from_lookup(&env(&[("FERROSA_SEED", "")]));
        assert!(issues.is_empty(), "{issues:?}");
        assert!(cfg.seeds.is_empty());
        assert!(NetConfig::from_lookup(&env(&[])).1.is_empty());
    }

    #[test]
    fn an_unparseable_bind_is_a_fatal_issue_and_keeps_the_default() {
        let (cfg, issues) = NetConfig::from_lookup(&env(&[("FERROSA_INTERNODE_BIND", "0.0.0.0")]));
        assert_eq!(cfg.bind_addr, NetConfig::default().bind_addr);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].var, "FERROSA_INTERNODE_BIND");
        assert_eq!(issues[0].value, "0.0.0.0");
        assert!(issues[0].fatal);
    }

    #[test]
    fn non_numeric_or_zero_tuning_values_are_fatal_issues() {
        for (var, value) in [
            ("FERROSA_HEARTBEAT_INTERVAL_MS", "fast"),
            ("FERROSA_HEARTBEAT_TIMEOUT_MS", "-1"),
            ("FERROSA_MAX_INTERNODE_CONNECTIONS", "many"),
            ("FERROSA_HANDSHAKE_TIMEOUT_SECS", "soon"),
            ("FERROSA_MAX_FRAME_BODY_SIZE", "big"),
            ("FERROSA_MAX_STREAMS_PER_LANE", "lots"),
            ("FERROSA_RAFT_LANE_TIMEOUT_MS", "0"),
            ("FERROSA_DATA_LANE_TIMEOUT_MS", "x"),
            ("FERROSA_DATA_LANE_MAX_IN_FLIGHT", "0"),
        ] {
            let (_, issues) = NetConfig::from_lookup(&env(&[(var, value)]));
            assert_eq!(issues.len(), 1, "{var}={value}: {issues:?}");
            assert_eq!(issues[0].var, var);
            assert!(issues[0].fatal, "{var}={value}");
        }
    }

    /// Any value other than a boolean used to read as `false`, so
    /// `FERROSA_INTERNODE_REQUIRE_TLS=ture` silently turned the TLS requirement off.
    #[test]
    fn a_non_boolean_require_tls_is_fatal_not_false() {
        for ok in ["true", "1", "false", "0"] {
            let (_, issues) =
                NetConfig::from_lookup(&env(&[("FERROSA_INTERNODE_REQUIRE_TLS", ok)]));
            assert!(issues.is_empty(), "{ok}: {issues:?}");
        }
        let (cfg, issues) =
            NetConfig::from_lookup(&env(&[("FERROSA_INTERNODE_REQUIRE_TLS", "ture")]));
        assert_eq!(issues.len(), 1);
        assert!(issues[0].fatal);
        assert!(
            !cfg.require_tls,
            "the lenient config still cannot claim TLS is required"
        );
    }

    #[test]
    fn an_unresolvable_seed_is_reported_but_not_fatal_and_the_rest_are_kept() {
        let (cfg, issues) = NetConfig::from_lookup(&env(&[(
            "FERROSA_SEED",
            "127.0.0.1:17000,no such host:17000",
        )]));
        assert_eq!(cfg.seeds, vec!["127.0.0.1:17000".parse().unwrap()]);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].var, "FERROSA_SEED");
        assert!(issues[0].value.contains("no such host"));
        assert!(!issues[0].fatal, "DNS may simply not be ready yet");
    }

    #[test]
    fn every_problem_is_reported_not_just_the_first() {
        let (_, issues) = NetConfig::from_lookup(&env(&[
            ("FERROSA_INTERNODE_BIND", "nope"),
            ("FERROSA_HEARTBEAT_INTERVAL_MS", "fast"),
            ("FERROSA_INTERNODE_REQUIRE_TLS", "ture"),
        ]));
        assert_eq!(issues.len(), 3, "{issues:?}");
    }

    #[test]
    fn checked_config_fails_on_fatal_issues_only() {
        let err = NetConfig::checked_from_lookup(&env(&[
            ("FERROSA_INTERNODE_BIND", "nope"),
            ("FERROSA_HEARTBEAT_INTERVAL_MS", "fast"),
        ]))
        .expect_err("typos must stop startup");
        assert!(err.contains("FERROSA_INTERNODE_BIND"), "{err}");
        assert!(
            err.contains("FERROSA_HEARTBEAT_INTERVAL_MS"),
            "all listed: {err}"
        );

        let ok = NetConfig::checked_from_lookup(&env(&[("FERROSA_SEED", "no such host:1")]));
        assert!(ok.is_ok(), "an unresolvable seed alone is not fatal");
    }

    #[test]
    fn parse_socket_addr_accepts_hostname_entries() {
        let addr =
            NetConfig::parse_socket_addr("localhost:7000").expect("localhost should resolve");
        assert!(addr.ip().is_loopback());
        assert_eq!(addr.port(), 7000);
    }

    #[test]
    fn seed_list_accepts_hostname_entries() {
        let (cfg, issues) =
            NetConfig::from_lookup(&env(&[("FERROSA_SEED", "localhost:7000, 127.0.0.1:7001")]));
        assert!(issues.is_empty(), "{issues:?}");
        let seeds = cfg.seeds;
        assert_eq!(seeds.len(), 2);
        assert!(seeds[0].ip().is_loopback());
        assert_eq!(seeds[0].port(), 7000);
        assert_eq!(seeds[1], "127.0.0.1:7001".parse().unwrap());
    }
}
