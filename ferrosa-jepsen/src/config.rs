use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Test tier — controls how many combinations are exercised.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
pub enum Tier {
    /// 3 nemeses x 16 LWT patterns, Rust driver only, T1 topology
    Smoke,
    /// T1+T2, 16 nemeses, all drivers, 2 concurrency levels
    Standard,
    /// All 4 topologies, 21 nemeses, all drivers, 3 concurrency levels
    Full,
    /// 24-hour continuous cycling on T4
    Endurance,
}

/// Cluster topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
pub enum Topology {
    /// 3-node single-DC (Firecracker)
    T1,
    /// 5-node single-DC (Firecracker)
    T2,
    /// 3+3 dual-DC (Fly.io)
    T3,
    /// 3+3+3 tri-DC (Fly.io)
    T4,
}

impl Topology {
    /// Number of nodes in this topology.
    pub fn node_count(self) -> usize {
        match self {
            Self::T1 => 3,
            Self::T2 => 5,
            Self::T3 => 6,
            Self::T4 => 9,
        }
    }

    /// Number of DCs.
    pub fn dc_count(self) -> usize {
        match self {
            Self::T1 | Self::T2 => 1,
            Self::T3 => 2,
            Self::T4 => 3,
        }
    }

    /// Quorum size (majority of nodes per DC for multi-DC, majority total for single-DC).
    pub fn quorum_size(self) -> usize {
        match self {
            Self::T1 => 2,
            Self::T2 => 3,
            Self::T3 => 2, // per-DC quorum of 3
            Self::T4 => 2, // per-DC quorum of 3
        }
    }

    /// Whether this topology requires Fly.io (vs local Firecracker).
    pub fn requires_fly(self) -> bool {
        matches!(self, Self::T3 | Self::T4)
    }
}

/// Concurrency level for workload generators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
pub enum Concurrency {
    /// 4 threads, 16 connections
    Low,
    /// 16 threads, 64 connections
    Medium,
    /// 64 threads, 256 connections
    High,
}

impl Concurrency {
    pub fn threads(self) -> usize {
        match self {
            Self::Low => 4,
            Self::Medium => 16,
            Self::High => 64,
        }
    }

    pub fn connections(self) -> usize {
        match self {
            Self::Low => 16,
            Self::Medium => 64,
            Self::High => 256,
        }
    }
}

/// Cluster backend — local Firecracker or Fly.io.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterBackend {
    Firecracker,
    FlyIo,
}

/// Full configuration for a test run.
#[derive(Debug, Clone)]
pub struct RunConfig {
    pub tier: Tier,
    pub topology: Option<Topology>,
    pub nemesis: Option<String>,
    pub pattern: Option<String>,
    pub driver: Option<String>,
    pub concurrency: Option<Concurrency>,
    pub run_id: String,
    pub output_dir: PathBuf,
    pub fly_regions: Vec<String>,
    pub alert_webhook: Option<String>,
    pub output_json: bool,
}

impl RunConfig {
    /// Resolve topologies for this run based on tier.
    pub fn topologies(&self) -> Vec<Topology> {
        if let Some(t) = self.topology {
            return vec![t];
        }
        match self.tier {
            Tier::Smoke => vec![Topology::T1],
            Tier::Standard => vec![Topology::T1, Topology::T2],
            Tier::Full => vec![Topology::T1, Topology::T2, Topology::T3, Topology::T4],
            Tier::Endurance => vec![Topology::T4],
        }
    }

    /// Resolve concurrency levels for this run.
    pub fn concurrency_levels(&self) -> Vec<Concurrency> {
        if let Some(c) = self.concurrency {
            return vec![c];
        }
        match self.tier {
            Tier::Smoke => vec![Concurrency::Low],
            Tier::Standard => vec![Concurrency::Low, Concurrency::Medium],
            Tier::Full | Tier::Endurance => {
                vec![Concurrency::Low, Concurrency::Medium, Concurrency::High]
            }
        }
    }

    /// Determine cluster backend from topology.
    pub fn backend_for(&self, topology: Topology) -> ClusterBackend {
        if topology.requires_fly() {
            ClusterBackend::FlyIo
        } else {
            ClusterBackend::Firecracker
        }
    }

    /// Workload run duration in seconds, based on tier.
    ///
    /// Smoke: short runs to verify correctness, not endurance.
    /// Endurance: 24-hour continuous cycling.
    pub fn run_duration_secs(&self) -> u64 {
        match self.tier {
            Tier::Smoke => 5,
            Tier::Standard => 30,
            Tier::Full => 60,
            Tier::Endurance => 86_400,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topology_t2_values() {
        assert_eq!(Topology::T2.node_count(), 5);
        assert_eq!(Topology::T2.dc_count(), 1);
        assert_eq!(Topology::T2.quorum_size(), 3);
        assert!(!Topology::T2.requires_fly());
    }

    #[test]
    fn topology_all_values() {
        assert_eq!(Topology::T1.node_count(), 3);
        assert_eq!(Topology::T2.node_count(), 5);
        assert_eq!(Topology::T3.node_count(), 6);
        assert_eq!(Topology::T4.node_count(), 9);
    }

    #[test]
    fn concurrency_values() {
        assert_eq!(Concurrency::Low.threads(), 4);
        assert_eq!(Concurrency::Medium.threads(), 16);
        assert_eq!(Concurrency::High.threads(), 64);
        assert_eq!(Concurrency::Low.connections(), 16);
        assert_eq!(Concurrency::High.connections(), 256);
    }

    #[test]
    fn run_config_topologies() {
        let config = RunConfig {
            tier: Tier::Standard,
            topology: None,
            nemesis: None,
            pattern: None,
            driver: None,
            concurrency: None,
            run_id: "test".into(),
            output_dir: PathBuf::from("/tmp"),
            fly_regions: vec![],
            alert_webhook: None,
            output_json: false,
        };
        let tops = config.topologies();
        assert_eq!(tops, vec![Topology::T1, Topology::T2]);
    }
}
