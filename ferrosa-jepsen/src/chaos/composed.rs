use anyhow::Result;
use async_trait::async_trait;

use super::{NemesisAction, NemesisContext};

/// Composed nemesis that runs multiple nemeses concurrently.
pub struct ComposedNemesis {
    name: String,
    components: Vec<Box<dyn NemesisAction>>,
}

impl ComposedNemesis {
    pub fn new(name: impl Into<String>, components: Vec<Box<dyn NemesisAction>>) -> Self {
        Self {
            name: name.into(),
            components,
        }
    }
}

#[async_trait]
impl NemesisAction for ComposedNemesis {
    fn name(&self) -> &str {
        &self.name
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        for c in &self.components {
            c.inject(ctx).await?;
        }
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        for c in &self.components {
            if let Err(e) = c.heal(ctx).await {
                tracing::warn!(component = c.name(), error = %e, "Heal failed for component");
            }
        }
        Ok(())
    }
}

/// Pre-defined composed nemesis: network partition + process kill.
pub fn partition_and_kill() -> ComposedNemesis {
    ComposedNemesis::new(
        "partition+kill",
        vec![
            Box::new(super::network::PartitionHalves),
            Box::new(super::process::KillMinority),
        ],
    )
}

/// Pre-defined composed nemesis: slow network + clock skew.
pub fn slow_and_clock() -> ComposedNemesis {
    ComposedNemesis::new(
        "slow+clock",
        vec![
            Box::new(super::network::SlowNetwork),
            Box::new(super::clock::ClockSkewSmall),
        ],
    )
}

/// Pre-defined composed nemesis: DC partition + process kill.
pub fn dc_partition_and_kill() -> ComposedNemesis {
    ComposedNemesis::new(
        "dc-partition+kill",
        vec![
            Box::new(super::wan_bridge::DcPartition),
            Box::new(super::process::KillMinority),
        ],
    )
}

/// Pre-defined composed nemesis: DC slow + disk slow.
pub fn dc_slow_and_disk() -> ComposedNemesis {
    ComposedNemesis::new(
        "dc-slow+disk",
        vec![
            Box::new(super::wan_bridge::DcSlow),
            Box::new(super::disk::DiskSlow),
        ],
    )
}

/// W7.11 — DC partition + DC slow composed nemesis. The
/// Sprint 7 acceptance test runs the bank workload at QUORUM under
/// this composed fault for 1 simulated hour on the T3 topology.
pub fn dc_partition_and_slow() -> ComposedNemesis {
    ComposedNemesis::new(
        "dc-partition+dc-slow",
        vec![
            Box::new(super::wan_bridge::DcPartition),
            Box::new(super::wan_bridge::DcSlow),
        ],
    )
}

/// Pre-defined composed nemesis: jitter + clock skew + pause.
pub fn everything() -> ComposedNemesis {
    ComposedNemesis::new(
        "everything",
        vec![
            Box::new(super::network::JitterNetwork),
            Box::new(super::clock::ClockSkewSmall),
            Box::new(super::process::PauseNode),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composed_nemesis_names() {
        assert_eq!(partition_and_kill().name(), "partition+kill");
        assert_eq!(everything().name(), "everything");
    }
}
