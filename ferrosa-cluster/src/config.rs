use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::consistency::ConsistencyLevel;

/// Role of this node in the cluster.
///
/// Controls whether the node owns data (token ranges), runs index builds,
/// or both. `Indexer`-only nodes don't serve reads/writes but offload
/// index builds from data nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Owns token ranges, serves reads and writes. Builds indexes locally.
    Data,
    /// Dedicated index builder. No token ownership, reads from S3.
    Indexer,
    /// Both data and indexer (default).
    Both,
}

/// Cluster configuration. Parsed from `FERROSA_*` environment variables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Cluster name — must match across all nodes.
    pub cluster_name: String,
    /// This node's data center.
    pub data_center: String,
    /// This node's rack within the data center.
    pub rack: String,
    /// Number of virtual token ranges per node.
    pub num_tokens: u32,
    /// Default consistency level for queries that don't specify one.
    pub default_cl: ConsistencyLevel,
    /// Hinted handoff storage directory.
    pub hinted_handoff_dir: PathBuf,
    /// Maximum hint storage per peer in megabytes.
    pub hinted_handoff_max_mb: u64,
    /// Allow unapproved nodes to join (true for development).
    pub auto_join: bool,
    /// Directory for Raft log store (sled). Defaults to `FERROSA_DATA_DIR/raft`.
    pub raft_data_dir: Option<PathBuf>,
    /// Role of this node: data, indexer, or both.
    pub node_role: NodeRole,
    /// Maximum seconds to wait in Forming state before falling back to Pair.
    /// `None` uses the default of 60 seconds.
    pub formation_timeout_secs: Option<u64>,
    /// CQL broadcast address (host:port) for system.peers.
    /// When set, overrides the internode address for native_address.
    /// Parsed from FERROSA_CQL_BROADCAST env var.
    pub cql_broadcast: Option<String>,
    /// Raft heartbeat interval in milliseconds. Default: 300.
    pub raft_heartbeat_ms: u64,
    /// Raft minimum election timeout in milliseconds. Default: 1000.
    pub raft_election_timeout_min_ms: u64,
    /// Raft maximum election timeout in milliseconds. Default: 2000.
    pub raft_election_timeout_max_ms: u64,
    /// Whether to enable PreVote (Ongaro §9.6) — ferrosa fork extension per
    /// ADR-012. Default: `true` (ferrosa default; upstream openraft 0.9 is
    /// `false`). Override with `FERROSA_RAFT_ENABLE_PRE_VOTE=false` to fall
    /// back to upstream behavior.
    pub raft_enable_pre_vote: bool,
    /// CheckQuorum step-down ratio (Ongaro §6.4) — ferrosa fork extension per
    /// ADR-012. Default: `0.75` (ferrosa default; upstream openraft 0.9
    /// effectively has CheckQuorum disabled). Override with
    /// `FERROSA_RAFT_CHECK_QUORUM_RATIO=<float>`. Set to `0.0` to disable.
    pub raft_check_quorum_ratio: f64,
    /// W6.8: per-DC config overrides for nodes hosting more than one
    /// Raft group (e.g., the W6.7 `bootstrap-dc` operator command
    /// adding a third DC on the same physical node). Each entry maps
    /// a DC name to its overrides; missing entries inherit the
    /// node-level defaults.
    ///
    /// Parsed from environment variables of the form
    /// `FERROSA_RAFT_DATA_DIR_<DC_NAME>`,
    /// `FERROSA_RACK_<DC_NAME>`, etc. Default: empty (single-DC
    /// behavior unchanged).
    #[serde(default)]
    pub per_dc_overrides: BTreeMap<String, PerDcOverride>,
}

/// W6.8: per-DC config overrides — each field is `Option<T>` so the
/// node-level default applies for unspecified fields.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PerDcOverride {
    /// Override for [`ClusterConfig::raft_data_dir`].
    pub raft_data_dir: Option<PathBuf>,
    /// Override for [`ClusterConfig::rack`].
    pub rack: Option<String>,
}

impl PerDcOverride {
    /// True iff every field is `None` — used by `from_env` to skip
    /// recording empty overrides.
    pub fn is_empty(&self) -> bool {
        self.raft_data_dir.is_none() && self.rack.is_none()
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            cluster_name: "ferrosa".to_string(),
            data_center: "datacenter1".to_string(),
            rack: "rack1".to_string(),
            num_tokens: 256,
            default_cl: ConsistencyLevel::Quorum,
            hinted_handoff_dir: PathBuf::from("data/hints"),
            hinted_handoff_max_mb: 1024,
            auto_join: true,
            raft_data_dir: None,
            node_role: NodeRole::Both,
            formation_timeout_secs: None,
            cql_broadcast: None,
            raft_heartbeat_ms: 300,
            raft_election_timeout_min_ms: 3000,
            raft_election_timeout_max_ms: 6000,
            raft_enable_pre_vote: true,
            raft_check_quorum_ratio: 0.75,
            per_dc_overrides: BTreeMap::new(),
        }
    }
}

impl ClusterConfig {
    /// Parse configuration from environment variables.
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(name) = std::env::var("FERROSA_CLUSTER_NAME") {
            config.cluster_name = name;
        }
        if let Ok(dc) = std::env::var("FERROSA_DATA_CENTER") {
            config.data_center = dc;
        }
        if let Ok(rack) = std::env::var("FERROSA_RACK") {
            config.rack = rack;
        }
        if let Ok(tokens) = std::env::var("FERROSA_NUM_TOKENS") {
            if let Ok(n) = tokens.parse() {
                config.num_tokens = n;
            }
        }
        if let Ok(cl) = std::env::var("FERROSA_DEFAULT_CL") {
            if let Some(parsed) = ConsistencyLevel::from_str(&cl) {
                config.default_cl = parsed;
            }
        }
        if let Ok(dir) = std::env::var("FERROSA_HINTED_HANDOFF_DIR") {
            config.hinted_handoff_dir = PathBuf::from(dir);
        }
        if let Ok(max) = std::env::var("FERROSA_HINTED_HANDOFF_MAX_MB") {
            if let Ok(n) = max.parse() {
                config.hinted_handoff_max_mb = n;
            }
        }
        if let Ok(auto) = std::env::var("FERROSA_AUTO_JOIN") {
            config.auto_join = auto == "true" || auto == "1";
        }
        if let Ok(timeout) = std::env::var("FERROSA_FORMATION_TIMEOUT_SECS") {
            if let Ok(n) = timeout.parse() {
                config.formation_timeout_secs = Some(n);
            }
        }
        if let Ok(role) = std::env::var("FERROSA_NODE_ROLE") {
            config.node_role = match role.to_lowercase().as_str() {
                "data" => NodeRole::Data,
                "indexer" => NodeRole::Indexer,
                "both" => NodeRole::Both,
                _ => NodeRole::Both,
            };
        }
        if let Ok(addr) = std::env::var("FERROSA_CQL_BROADCAST") {
            config.cql_broadcast = Some(addr);
        }
        if let Ok(dir) = std::env::var("FERROSA_RAFT_DATA_DIR") {
            config.raft_data_dir = Some(PathBuf::from(dir));
        }
        if let Ok(val) = std::env::var("FERROSA_RAFT_HEARTBEAT_MS") {
            if let Ok(n) = val.parse() {
                config.raft_heartbeat_ms = n;
            }
        }
        if let Ok(val) = std::env::var("FERROSA_RAFT_ELECTION_MIN_MS") {
            if let Ok(n) = val.parse() {
                config.raft_election_timeout_min_ms = n;
            }
        }
        if let Ok(val) = std::env::var("FERROSA_RAFT_ELECTION_MAX_MS") {
            if let Ok(n) = val.parse() {
                config.raft_election_timeout_max_ms = n;
            }
        }
        if let Ok(val) = std::env::var("FERROSA_RAFT_ENABLE_PRE_VOTE") {
            // Accept "true"/"false"/"1"/"0".
            match val.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => config.raft_enable_pre_vote = true,
                "false" | "0" | "no" => config.raft_enable_pre_vote = false,
                other => {
                    tracing::warn!(
                        value = %other,
                        "FERROSA_RAFT_ENABLE_PRE_VOTE: unrecognized value, keeping default"
                    );
                }
            }
        }
        if let Ok(val) = std::env::var("FERROSA_RAFT_CHECK_QUORUM_RATIO") {
            match val.parse::<f64>() {
                Ok(ratio) if (0.0..=2.0).contains(&ratio) => {
                    config.raft_check_quorum_ratio = ratio;
                }
                Ok(ratio) => {
                    tracing::warn!(
                        ratio,
                        "FERROSA_RAFT_CHECK_QUORUM_RATIO: out of range [0.0, 2.0], keeping default"
                    );
                }
                Err(e) => {
                    tracing::warn!(%e, value = %val, "FERROSA_RAFT_CHECK_QUORUM_RATIO: parse error, keeping default");
                }
            }
        }

        // W6.8: per-DC overrides. Pulled from the live env so we
        // don't need to hard-code a DC list — every env var matching
        // `FERROSA_RAFT_DATA_DIR_<NAME>` or `FERROSA_RACK_<NAME>` (with
        // `<NAME>` non-empty) populates an entry.
        config.per_dc_overrides = parse_per_dc_overrides_from_env();

        config
    }

    /// Effective `raft_data_dir` for a DC, honoring per-DC overrides.
    ///
    /// Returns the DC-specific override if one is configured;
    /// otherwise the node-level [`Self::raft_data_dir`].
    pub fn raft_data_dir_for_dc(&self, dc_name: &str) -> Option<PathBuf> {
        if let Some(o) = self.per_dc_overrides.get(dc_name) {
            if let Some(d) = &o.raft_data_dir {
                return Some(d.clone());
            }
        }
        self.raft_data_dir.clone()
    }

    /// Effective `rack` for a DC, honoring per-DC overrides.
    pub fn rack_for_dc(&self, dc_name: &str) -> String {
        if let Some(o) = self.per_dc_overrides.get(dc_name) {
            if let Some(r) = &o.rack {
                return r.clone();
            }
        }
        self.rack.clone()
    }
}

/// Parse per-DC overrides from the live process environment.
///
/// Recognised patterns (case-sensitive):
///
/// - `FERROSA_RAFT_DATA_DIR_<DC>` — path for that DC's sled log.
/// - `FERROSA_RACK_<DC>` — rack name within that DC.
///
/// `<DC>` is the literal DC name; characters allowed in env-var
/// names are constrained, so this is intended for ASCII DC names
/// like `dc1`, `us-east`, `eu_west_2`, etc. (the env-var var name
/// must remain a valid `[A-Za-z0-9_]+`; operators must use `_` in
/// place of `-` if their DC names contain dashes).
fn parse_per_dc_overrides_from_env() -> BTreeMap<String, PerDcOverride> {
    let mut overrides: BTreeMap<String, PerDcOverride> = BTreeMap::new();
    for (key, value) in std::env::vars() {
        let dc_opt = key
            .strip_prefix("FERROSA_RAFT_DATA_DIR_")
            .map(|dc| ("raft_data_dir", dc))
            .or_else(|| key.strip_prefix("FERROSA_RACK_").map(|dc| ("rack", dc)));
        let Some((field, dc)) = dc_opt else {
            continue;
        };
        if dc.is_empty() {
            continue;
        }
        let entry = overrides.entry(dc.to_string()).or_default();
        match field {
            "raft_data_dir" => entry.raft_data_dir = Some(PathBuf::from(value)),
            "rack" => entry.rack = Some(value),
            _ => unreachable!(),
        }
    }
    // Drop any entries that ended up empty (e.g., env contained only
    // an explicitly empty value).
    overrides.retain(|_, v| !v.is_empty());
    overrides
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let config = ClusterConfig::default();
        assert_eq!(config.cluster_name, "ferrosa");
        assert_eq!(config.data_center, "datacenter1");
        assert_eq!(config.rack, "rack1");
        assert_eq!(config.num_tokens, 256);
        assert_eq!(config.default_cl, ConsistencyLevel::Quorum);
        assert_eq!(config.hinted_handoff_max_mb, 1024);
        assert!(config.auto_join);
    }

    /// W3.12 / ADR-012: ferrosa defaults flip the openraft knobs ON.
    #[test]
    fn default_raft_correctness_knobs_match_adr_012() {
        let config = ClusterConfig::default();
        assert!(
            config.raft_enable_pre_vote,
            "ADR-012: PreVote must be on by default in ferrosa builds"
        );
        assert_eq!(
            config.raft_check_quorum_ratio, 0.75,
            "ADR-012: CheckQuorum default ratio is 0.75 (not etcd's 1.0)"
        );
    }

    #[test]
    fn node_role_default_is_both() {
        let config = ClusterConfig::default();
        assert_eq!(config.node_role, NodeRole::Both);
    }

    #[test]
    fn node_role_serde_roundtrip() {
        for role in [NodeRole::Data, NodeRole::Indexer, NodeRole::Both] {
            let json = serde_json::to_string(&role).unwrap();
            let back: NodeRole = serde_json::from_str(&json).unwrap();
            assert_eq!(back, role);
        }
    }

    #[test]
    fn cluster_config_serde_with_node_role() {
        let config = ClusterConfig {
            node_role: NodeRole::Indexer,
            ..ClusterConfig::default()
        };

        let bytes = bincode::serialize(&config).unwrap();
        let decoded: ClusterConfig = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.node_role, NodeRole::Indexer);
    }

    /// W6.8 RED: per-DC config carries independent raft_data_dir +
    /// rack overrides. Default node-level fields apply when no per-DC
    /// override exists.
    #[test]
    fn cluster_config_per_dc() {
        let mut overrides = BTreeMap::new();
        overrides.insert(
            "dc1".to_string(),
            PerDcOverride {
                raft_data_dir: Some(PathBuf::from("/var/lib/ferrosa/raft-dc1")),
                rack: Some("rack-east".to_string()),
            },
        );
        overrides.insert(
            "dc2".to_string(),
            PerDcOverride {
                raft_data_dir: Some(PathBuf::from("/var/lib/ferrosa/raft-dc2")),
                rack: None,
            },
        );

        let config = ClusterConfig {
            data_center: "dc1".to_string(),
            rack: "rack-default".to_string(),
            raft_data_dir: Some(PathBuf::from("/var/lib/ferrosa/raft-default")),
            per_dc_overrides: overrides,
            ..ClusterConfig::default()
        };

        // dc1 has both override fields.
        assert_eq!(
            config.raft_data_dir_for_dc("dc1"),
            Some(PathBuf::from("/var/lib/ferrosa/raft-dc1"))
        );
        assert_eq!(config.rack_for_dc("dc1"), "rack-east");

        // dc2 only overrides raft_data_dir; rack falls through to
        // node-level default.
        assert_eq!(
            config.raft_data_dir_for_dc("dc2"),
            Some(PathBuf::from("/var/lib/ferrosa/raft-dc2"))
        );
        assert_eq!(config.rack_for_dc("dc2"), "rack-default");

        // Unknown DC: both fall through.
        assert_eq!(
            config.raft_data_dir_for_dc("dc-unknown"),
            Some(PathBuf::from("/var/lib/ferrosa/raft-default"))
        );
        assert_eq!(config.rack_for_dc("dc-unknown"), "rack-default");
    }

    /// W6.8 RED: backward-compat — when no per-DC overrides are
    /// configured (the default), a single-DC cluster behaves
    /// exactly like today's clusters: node-level fields apply for
    /// every DC name.
    #[test]
    fn single_dc_default_keeps_node_level_fields_unchanged() {
        let config = ClusterConfig {
            rack: "rack9".to_string(),
            raft_data_dir: Some(PathBuf::from("/data/raft")),
            ..ClusterConfig::default()
        };
        assert!(config.per_dc_overrides.is_empty());
        assert_eq!(
            config.raft_data_dir_for_dc("default"),
            Some(PathBuf::from("/data/raft"))
        );
        assert_eq!(config.rack_for_dc("default"), "rack9");
    }

    /// W6.8: per-DC overrides round-trip through serde.
    #[test]
    fn per_dc_overrides_serde_roundtrip() {
        let mut overrides = BTreeMap::new();
        overrides.insert(
            "dc1".to_string(),
            PerDcOverride {
                raft_data_dir: Some(PathBuf::from("/raft/dc1")),
                rack: Some("rack-1".to_string()),
            },
        );
        let config = ClusterConfig {
            per_dc_overrides: overrides,
            ..ClusterConfig::default()
        };
        let bytes = bincode::serialize(&config).expect("ser");
        let back: ClusterConfig = bincode::deserialize(&bytes).expect("de");
        assert_eq!(back.per_dc_overrides.len(), 1);
        let dc1 = back.per_dc_overrides.get("dc1").unwrap();
        assert_eq!(dc1.raft_data_dir, Some(PathBuf::from("/raft/dc1")));
        assert_eq!(dc1.rack.as_deref(), Some("rack-1"));
    }

    /// W6.8: `parse_per_dc_overrides_from_env` reads `FERROSA_RAFT_DATA_DIR_<DC>`
    /// and `FERROSA_RACK_<DC>` and groups them by DC. Uses a shared
    /// global env, so this test sets/clears the vars within its own
    /// process scope.
    #[test]
    fn parse_per_dc_overrides_from_env_groups_by_dc() {
        // Env mutation is process-global; use a unique DC name so we
        // don't race with other tests.
        let dc = format!("env_dc_{}", uuid::Uuid::new_v4().simple());
        let raft_var = format!("FERROSA_RAFT_DATA_DIR_{dc}");
        let rack_var = format!("FERROSA_RACK_{dc}");
        // SAFETY: env vars are inherently process-global; this test
        // is single-threaded with respect to its own keys.
        std::env::set_var(&raft_var, "/raft/from-env");
        std::env::set_var(&rack_var, "rack-env");

        let overrides = parse_per_dc_overrides_from_env();
        let entry = overrides
            .get(&dc)
            .expect("env override grouped under DC name");
        assert_eq!(entry.raft_data_dir, Some(PathBuf::from("/raft/from-env")));
        assert_eq!(entry.rack.as_deref(), Some("rack-env"));

        std::env::remove_var(&raft_var);
        std::env::remove_var(&rack_var);
    }
}
