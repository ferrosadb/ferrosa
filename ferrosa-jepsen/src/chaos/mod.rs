pub mod clock;
pub mod composed;
pub mod disk;
pub mod network;
pub mod process;
pub mod wan_bridge;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A nemesis action that can be injected and healed.
#[async_trait]
pub trait NemesisAction: Send + Sync {
    /// Human-readable name of this nemesis.
    fn name(&self) -> &str;

    /// Inject the failure.
    async fn inject(&self, ctx: &NemesisContext) -> Result<()>;

    /// Heal/recover from the failure.
    async fn heal(&self, ctx: &NemesisContext) -> Result<()>;
}

/// Context provided to nemesis actions -- node IPs, SSH access, etc.
pub struct NemesisContext {
    /// All node IPs in the cluster.
    pub node_ips: Vec<String>,
    /// SSH user for connecting to nodes.
    pub ssh_user: String,
    /// SSH key path.
    pub ssh_key_path: PathBuf,
    /// SSH port.
    pub ssh_port: u16,
}

impl NemesisContext {
    /// Get IPs for "majority" partition (more than half).
    pub fn majority_ips(&self) -> Vec<String> {
        let n = (self.node_ips.len() / 2) + 1;
        self.node_ips[..n].to_vec()
    }

    /// Get IPs for "minority" partition (less than half).
    pub fn minority_ips(&self) -> Vec<String> {
        let n = (self.node_ips.len() / 2) + 1;
        self.node_ips[n..].to_vec()
    }

    /// Connect SSH to a specific node IP.
    pub async fn ssh_to(&self, ip: &str) -> Result<crate::ssh::SshClient> {
        crate::ssh::SshClient::connect(ip, self.ssh_port, &self.ssh_user, &self.ssh_key_path).await
    }
}

/// Schedule for running nemeses during a test.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NemesisSchedule {
    /// Name of nemesis to use.
    pub nemesis_name: String,
    /// How long the failure lasts.
    pub inject_duration: Duration,
    /// How long between injections.
    pub heal_duration: Duration,
    /// Total number of inject/heal cycles.
    pub cycles: usize,
}

/// Registry of available nemeses.
pub struct NemesisRegistry {
    nemeses: std::collections::HashMap<String, Box<dyn NemesisAction>>,
}

impl NemesisRegistry {
    pub fn new() -> Self {
        Self {
            nemeses: std::collections::HashMap::new(),
        }
    }

    pub fn register(&mut self, nemesis: Box<dyn NemesisAction>) {
        let name = nemesis.name().to_string();
        self.nemeses.insert(name, nemesis);
    }

    pub fn get(&self, name: &str) -> Option<&dyn NemesisAction> {
        self.nemeses.get(name).map(|b| b.as_ref())
    }

    pub fn names(&self) -> Vec<String> {
        self.nemeses.keys().cloned().collect()
    }

    /// Create a registry with all Phase 1 nemeses pre-registered.
    pub fn phase1() -> Self {
        let mut reg = Self::new();
        reg.register(Box::new(network::PartitionHalves));
        reg.register(Box::new(process::KillMinority));
        reg.register(Box::new(clock::ClockSkewSmall));
        reg
    }

    /// Create a registry with all 16 Phase 2 nemeses pre-registered.
    pub fn phase2() -> Self {
        let mut reg = Self::phase1();
        // Network (5 more)
        reg.register(Box::new(network::PartitionRing));
        reg.register(Box::new(network::PartitionOne));
        reg.register(Box::new(network::SlowNetwork));
        reg.register(Box::new(network::JitterNetwork));
        reg.register(Box::new(network::PacketLoss));
        // Process (2 more)
        reg.register(Box::new(process::KillMajority));
        reg.register(Box::new(process::PauseNode));
        // Clock (2 more)
        reg.register(Box::new(clock::ClockSkewLarge));
        reg.register(Box::new(clock::ClockStrobe));
        // Disk (2)
        reg.register(Box::new(disk::DiskSlow));
        reg.register(Box::new(disk::DiskFail));
        reg
    }

    /// Phase 4: all nemeses including WAN and composed.
    pub fn full() -> Self {
        let mut reg = Self::phase2();
        // WAN nemeses (5)
        reg.register(Box::new(wan_bridge::DcPartition));
        reg.register(Box::new(wan_bridge::DcSlow));
        reg.register(Box::new(wan_bridge::DcAsymmetric));
        reg.register(Box::new(wan_bridge::DcFlap));
        reg.register(Box::new(wan_bridge::DcLossy));
        // Composed (5)
        reg.register(Box::new(composed::partition_and_kill()));
        reg.register(Box::new(composed::slow_and_clock()));
        reg.register(Box::new(composed::dc_partition_and_kill()));
        reg.register(Box::new(composed::dc_slow_and_disk()));
        reg.register(Box::new(composed::everything()));
        reg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nemesis_registry_phase1() {
        let reg = NemesisRegistry::phase1();
        let mut names = reg.names();
        names.sort();
        assert_eq!(
            names,
            vec!["clock-skew-small", "kill-minority", "partition-halves"]
        );
    }

    #[test]
    fn nemesis_registry_phase2() {
        let reg = NemesisRegistry::phase2();
        let mut names = reg.names();
        names.sort();
        assert_eq!(names.len(), 14);
        assert_eq!(
            names,
            vec![
                "clock-skew-large",
                "clock-skew-small",
                "clock-strobe",
                "disk-fail",
                "disk-slow",
                "jitter-network",
                "kill-majority",
                "kill-minority",
                "packet-loss",
                "partition-halves",
                "partition-one",
                "partition-ring",
                "pause-node",
                "slow-network",
            ]
        );
    }

    #[test]
    fn nemesis_registry_full() {
        let reg = NemesisRegistry::full();
        let names = reg.names();
        assert!(names.len() >= 24); // 14 phase2 + 5 WAN + 5 composed
        assert!(names.contains(&"dc-partition".to_string()));
        assert!(names.contains(&"partition+kill".to_string()));
    }

    #[test]
    fn nemesis_context_partitioning() {
        let ctx = NemesisContext {
            node_ips: vec!["1".into(), "2".into(), "3".into()],
            ssh_user: "root".into(),
            ssh_key_path: PathBuf::from("/tmp/key"),
            ssh_port: 22,
        };
        assert_eq!(ctx.majority_ips(), vec!["1", "2"]);
        assert_eq!(ctx.minority_ips(), vec!["3"]);
    }
}
