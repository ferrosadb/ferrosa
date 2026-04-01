use serde::{Deserialize, Serialize};

/// Deployment mode tracking the cluster formation lifecycle.
///
/// Progressive formation: Standalone → Pair → Forming → Cluster.
/// Degraded states preserve context for automatic recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeploymentMode {
    Standalone,
    Pair,
    /// Intermediate state: 2nd peer seen, mesh forming, awaiting Raft init.
    Forming,
    Cluster,
    /// Pair peer lost. Preserves peer context for recovery.
    DegradedPair,
    /// Cluster quorum lost. Raft running but cannot commit.
    DegradedCluster,
}

impl DeploymentMode {
    /// Infer deployment mode from the number of peers (excluding self).
    pub fn from_peer_count(count: usize) -> Self {
        match count {
            0 => Self::Standalone,
            1 => Self::Pair,
            _ => Self::Cluster,
        }
    }

    /// Check if transitioning from `self` to `target` is allowed.
    ///
    /// Valid transitions:
    /// ```text
    /// Standalone → Pair
    /// Pair → Forming | DegradedPair
    /// Forming → Cluster | Pair (fallback on timeout)
    /// Cluster → DegradedCluster
    /// DegradedPair → Pair (recovery) | Standalone (operator demote)
    /// DegradedCluster → Cluster (quorum restored)
    /// ```
    /// Legacy: Standalone → Cluster and Pair → Cluster are kept for
    /// backward compatibility but new code should go through Forming.
    pub fn can_transition_to(&self, target: Self) -> bool {
        matches!(
            (self, target),
            // Forward progression
            (Self::Standalone, Self::Pair)
                | (Self::Pair, Self::Forming)
                | (Self::Forming, Self::Cluster)
                // Forming fallback (3rd node disappeared)
                | (Self::Forming, Self::Pair)
                // Degraded transitions
                | (Self::Pair, Self::DegradedPair)
                | (Self::Cluster, Self::DegradedCluster)
                // Recovery
                | (Self::DegradedPair, Self::Pair)
                | (Self::DegradedCluster, Self::Cluster)
                // Operator demote from degraded pair
                | (Self::DegradedPair, Self::Standalone)
                // Legacy direct transitions (backward compat)
                | (Self::Standalone, Self::Cluster)
                | (Self::Pair, Self::Cluster)
        )
    }
}

impl std::fmt::Display for DeploymentMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standalone => write!(f, "standalone"),
            Self::Pair => write!(f, "pair"),
            Self::Forming => write!(f, "forming"),
            Self::Cluster => write!(f, "cluster"),
            Self::DegradedPair => write!(f, "degraded-pair"),
            Self::DegradedCluster => write!(f, "degraded-cluster"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_peer_count() {
        assert_eq!(
            DeploymentMode::from_peer_count(0),
            DeploymentMode::Standalone
        );
        assert_eq!(DeploymentMode::from_peer_count(1), DeploymentMode::Pair);
        assert_eq!(DeploymentMode::from_peer_count(2), DeploymentMode::Cluster);
        assert_eq!(DeploymentMode::from_peer_count(10), DeploymentMode::Cluster);
    }

    #[test]
    fn transitions_are_one_way() {
        assert!(DeploymentMode::Standalone.can_transition_to(DeploymentMode::Pair));
        assert!(DeploymentMode::Standalone.can_transition_to(DeploymentMode::Cluster));
        assert!(DeploymentMode::Pair.can_transition_to(DeploymentMode::Cluster));
        assert!(!DeploymentMode::Pair.can_transition_to(DeploymentMode::Standalone));
        assert!(!DeploymentMode::Cluster.can_transition_to(DeploymentMode::Pair));
        assert!(!DeploymentMode::Cluster.can_transition_to(DeploymentMode::Standalone));
    }

    // --- Formation state machine tests (S1.1) ---

    #[test]
    fn forming_state_exists() {
        // Forming is an intermediate state between Pair and Cluster
        let mode = DeploymentMode::Forming;
        assert_eq!(format!("{mode}"), "forming");
    }

    #[test]
    fn degraded_pair_state_exists() {
        let mode = DeploymentMode::DegradedPair;
        assert_eq!(format!("{mode}"), "degraded-pair");
    }

    #[test]
    fn degraded_cluster_state_exists() {
        let mode = DeploymentMode::DegradedCluster;
        assert_eq!(format!("{mode}"), "degraded-cluster");
    }

    #[test]
    fn pair_to_forming_transition() {
        assert!(DeploymentMode::Pair.can_transition_to(DeploymentMode::Forming));
    }

    #[test]
    fn forming_to_cluster_transition() {
        assert!(DeploymentMode::Forming.can_transition_to(DeploymentMode::Cluster));
    }

    #[test]
    fn forming_falls_back_to_pair() {
        // If 3rd node disappears, Forming should fall back to Pair
        assert!(DeploymentMode::Forming.can_transition_to(DeploymentMode::Pair));
    }

    #[test]
    fn pair_to_degraded_pair() {
        assert!(DeploymentMode::Pair.can_transition_to(DeploymentMode::DegradedPair));
    }

    #[test]
    fn degraded_pair_recovers_to_pair() {
        assert!(DeploymentMode::DegradedPair.can_transition_to(DeploymentMode::Pair));
    }

    #[test]
    fn degraded_pair_demotes_to_standalone() {
        assert!(DeploymentMode::DegradedPair.can_transition_to(DeploymentMode::Standalone));
    }

    #[test]
    fn cluster_to_degraded_cluster() {
        assert!(DeploymentMode::Cluster.can_transition_to(DeploymentMode::DegradedCluster));
    }

    #[test]
    fn degraded_cluster_recovers_to_cluster() {
        assert!(DeploymentMode::DegradedCluster.can_transition_to(DeploymentMode::Cluster));
    }

    #[test]
    fn forming_cannot_go_to_standalone() {
        assert!(!DeploymentMode::Forming.can_transition_to(DeploymentMode::Standalone));
    }

    #[test]
    fn cluster_cannot_go_to_pair() {
        assert!(!DeploymentMode::Cluster.can_transition_to(DeploymentMode::Pair));
    }

    #[test]
    fn degraded_cluster_cannot_go_to_pair() {
        assert!(!DeploymentMode::DegradedCluster.can_transition_to(DeploymentMode::Pair));
    }

    #[test]
    fn all_modes_display_correctly() {
        assert_eq!(format!("{}", DeploymentMode::Standalone), "standalone");
        assert_eq!(format!("{}", DeploymentMode::Pair), "pair");
        assert_eq!(format!("{}", DeploymentMode::Forming), "forming");
        assert_eq!(format!("{}", DeploymentMode::Cluster), "cluster");
        assert_eq!(format!("{}", DeploymentMode::DegradedPair), "degraded-pair");
        assert_eq!(format!("{}", DeploymentMode::DegradedCluster), "degraded-cluster");
    }

    #[test]
    fn all_modes_serialize_deserialize() {
        for mode in [
            DeploymentMode::Standalone,
            DeploymentMode::Pair,
            DeploymentMode::Forming,
            DeploymentMode::Cluster,
            DeploymentMode::DegradedPair,
            DeploymentMode::DegradedCluster,
        ] {
            let json = serde_json::to_string(&mode).unwrap();
            let back: DeploymentMode = serde_json::from_str(&json).unwrap();
            assert_eq!(mode, back);
        }
    }
}
