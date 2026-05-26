//! Replication strategies for replica selection.
//!
//! Phase 2: SimpleStrategy (single DC).
//! Phase 3: NetworkTopologyStrategy (multi-DC).

use std::collections::HashMap;

/// Replication strategy determines how replicas are selected.
#[derive(Debug, Clone)]
pub enum ReplicationStrategy {
    Simple { replication_factor: usize },
    NetworkTopology { dc_rf: HashMap<String, usize> },
}

impl ReplicationStrategy {
    /// Total replication factor across all data centers.
    ///
    /// For `Simple`, returns the single RF. For `NetworkTopology`, returns
    /// the sum of all per-DC replication factors.
    pub fn replication_factor(&self) -> usize {
        match self {
            Self::Simple { replication_factor } => *replication_factor,
            Self::NetworkTopology { dc_rf } => dc_rf.values().sum(),
        }
    }

    /// Per-DC replication factors. Returns an empty map for `Simple`.
    pub fn dc_replication_factors(&self) -> &HashMap<String, usize> {
        // Use a static empty map for the Simple case to avoid allocation.
        static EMPTY: std::sync::LazyLock<HashMap<String, usize>> =
            std::sync::LazyLock::new(HashMap::new);
        match self {
            Self::Simple { .. } => &EMPTY,
            Self::NetworkTopology { dc_rf } => dc_rf,
        }
    }
}

use ferrosa_schema::metadata::keyspace::ReplicationParams;

/// Error parsing a [`ReplicationStrategy`] from [`ReplicationParams`].
#[derive(Debug, Clone)]
pub struct StrategyParseError(pub String);

impl std::fmt::Display for StrategyParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid replication strategy: {}", self.0)
    }
}

impl std::error::Error for StrategyParseError {}

impl TryFrom<&ReplicationParams> for ReplicationStrategy {
    type Error = StrategyParseError;

    fn try_from(params: &ReplicationParams) -> Result<Self, Self::Error> {
        match params.strategy.as_str() {
            "SimpleStrategy" | "org.apache.cassandra.locator.SimpleStrategy" => {
                let rf_str = params.options.get("replication_factor").ok_or_else(|| {
                    StrategyParseError("SimpleStrategy requires 'replication_factor'".into())
                })?;
                let rf: usize = rf_str.parse().map_err(|_| {
                    StrategyParseError(format!("invalid replication_factor: '{rf_str}'"))
                })?;
                Ok(Self::Simple {
                    replication_factor: rf,
                })
            }
            "NetworkTopologyStrategy" | "org.apache.cassandra.locator.NetworkTopologyStrategy" => {
                let mut dc_rf = HashMap::new();
                for (k, v) in &params.options {
                    if k == "class" {
                        continue;
                    }
                    let rf: usize = v.parse().map_err(|_| {
                        StrategyParseError(format!(
                            "invalid replication factor for DC '{k}': '{v}'"
                        ))
                    })?;
                    dc_rf.insert(k.clone(), rf);
                }
                Ok(Self::NetworkTopology { dc_rf })
            }
            other => Err(StrategyParseError(format!("unknown strategy: '{other}'"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_strategy_rf() {
        let s = ReplicationStrategy::Simple {
            replication_factor: 3,
        };
        assert_eq!(s.replication_factor(), 3);
    }

    #[test]
    fn network_topology_total_rf() {
        let dc_rf: HashMap<String, usize> =
            HashMap::from([("dc1".to_string(), 3), ("dc2".to_string(), 2)]);
        let s = ReplicationStrategy::NetworkTopology {
            dc_rf: dc_rf.clone(),
        };
        assert_eq!(s.replication_factor(), 5);
        assert_eq!(s.dc_replication_factors(), &dc_rf);
    }

    #[test]
    fn network_topology_single_dc() {
        let dc_rf = HashMap::from([("us-east-1".to_string(), 3)]);
        let s = ReplicationStrategy::NetworkTopology { dc_rf };
        assert_eq!(s.replication_factor(), 3);
    }

    #[test]
    fn network_topology_empty_dc_rf() {
        let s = ReplicationStrategy::NetworkTopology {
            dc_rf: HashMap::new(),
        };
        assert_eq!(s.replication_factor(), 0);
    }

    #[test]
    fn simple_dc_replication_factors_is_empty() {
        let s = ReplicationStrategy::Simple {
            replication_factor: 3,
        };
        assert!(s.dc_replication_factors().is_empty());
    }

    use ferrosa_schema::metadata::keyspace::ReplicationParams;

    #[test]
    fn parse_simple_strategy_from_params() {
        let params = ReplicationParams {
            strategy: "SimpleStrategy".to_string(),
            options: HashMap::from([("replication_factor".to_string(), "3".to_string())]),
        };
        let strategy = ReplicationStrategy::try_from(&params).unwrap();
        assert_eq!(strategy.replication_factor(), 3);
        assert!(matches!(
            strategy,
            ReplicationStrategy::Simple {
                replication_factor: 3
            }
        ));
    }

    #[test]
    fn parse_nts_from_params() {
        let params = ReplicationParams {
            strategy: "NetworkTopologyStrategy".to_string(),
            options: HashMap::from([
                ("dc1".to_string(), "3".to_string()),
                ("dc2".to_string(), "2".to_string()),
            ]),
        };
        let strategy = ReplicationStrategy::try_from(&params).unwrap();
        assert_eq!(strategy.replication_factor(), 5);
        match &strategy {
            ReplicationStrategy::NetworkTopology { dc_rf } => {
                assert_eq!(dc_rf.get("dc1"), Some(&3));
                assert_eq!(dc_rf.get("dc2"), Some(&2));
            }
            other => panic!("expected NetworkTopology, got {:?}", other),
        }
    }

    #[test]
    fn parse_nts_ignores_cql_class_option() {
        let params = ReplicationParams {
            strategy: "NetworkTopologyStrategy".to_string(),
            options: HashMap::from([
                (
                    "class".to_string(),
                    "org.apache.cassandra.locator.NetworkTopologyStrategy".to_string(),
                ),
                ("datacenter1".to_string(), "3".to_string()),
            ]),
        };
        let strategy = ReplicationStrategy::try_from(&params).unwrap();
        assert_eq!(strategy.replication_factor(), 3);
        match &strategy {
            ReplicationStrategy::NetworkTopology { dc_rf } => {
                assert_eq!(dc_rf.get("datacenter1"), Some(&3));
                assert!(!dc_rf.contains_key("class"));
            }
            other => panic!("expected NetworkTopology, got {:?}", other),
        }
    }

    #[test]
    fn parse_unknown_strategy_fails() {
        let params = ReplicationParams {
            strategy: "UnknownStrategy".to_string(),
            options: HashMap::new(),
        };
        assert!(ReplicationStrategy::try_from(&params).is_err());
    }

    #[test]
    fn parse_nts_non_numeric_rf_fails() {
        let params = ReplicationParams {
            strategy: "NetworkTopologyStrategy".to_string(),
            options: HashMap::from([("dc1".to_string(), "abc".to_string())]),
        };
        assert!(ReplicationStrategy::try_from(&params).is_err());
    }
}
