use serde::{Deserialize, Serialize};

/// Deployment mode inferred from peer count or set explicitly.
///
/// Mode transitions are a one-way ratchet: Standalone → Pair → Cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeploymentMode {
    Standalone,
    Pair,
    Cluster,
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
    /// Transitions are one-way: Standalone → Pair → Cluster.
    pub fn can_transition_to(&self, target: Self) -> bool {
        matches!(
            (self, target),
            (Self::Standalone, Self::Pair)
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
            Self::Cluster => write!(f, "cluster"),
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
}
