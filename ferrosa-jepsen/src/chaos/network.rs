use anyhow::Result;
use async_trait::async_trait;
use rand::seq::IndexedRandom;

use super::{NemesisAction, NemesisContext};

/// Partition the cluster into two halves using iptables.
///
/// Majority partition keeps communicating; minority is isolated.
pub struct PartitionHalves;

#[async_trait]
impl NemesisAction for PartitionHalves {
    fn name(&self) -> &str {
        "partition-halves"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        let majority = ctx.majority_ips();
        let minority = ctx.minority_ips();

        // On each minority node, drop packets to/from majority nodes.
        for minority_ip in &minority {
            let ssh = ctx.ssh_to(minority_ip).await?;
            for majority_ip in &majority {
                // Drop incoming from majority.
                ssh.exec(&format!("iptables -A INPUT -s {majority_ip} -j DROP"))
                    .await?;
                // Drop outgoing to majority.
                ssh.exec(&format!("iptables -A OUTPUT -d {majority_ip} -j DROP"))
                    .await?;
            }
        }

        // Also on majority nodes, drop packets from minority.
        for majority_ip in &majority {
            let ssh = ctx.ssh_to(majority_ip).await?;
            for minority_ip in &minority {
                ssh.exec(&format!("iptables -A INPUT -s {minority_ip} -j DROP"))
                    .await?;
                ssh.exec(&format!("iptables -A OUTPUT -d {minority_ip} -j DROP"))
                    .await?;
            }
        }

        tracing::info!(
            majority = ?majority,
            minority = ?minority,
            "Injected partition-halves"
        );
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        // Flush all iptables rules on all nodes.
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("iptables -F INPUT").await?;
            ssh.exec("iptables -F OUTPUT").await?;
        }
        tracing::info!("Healed partition-halves");
        Ok(())
    }
}

/// Partition into a ring: each node can only talk to its neighbors.
pub struct PartitionRing;

#[async_trait]
impl NemesisAction for PartitionRing {
    fn name(&self) -> &str {
        "partition-ring"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        let ips = &ctx.node_ips;
        let n = ips.len();
        assert!(n >= 3, "ring partition requires at least 3 nodes");

        // For each node, drop traffic to/from non-adjacent nodes in the ring.
        for i in 0..n {
            let prev = (i + n - 1) % n;
            let next = (i + 1) % n;
            let ssh = ctx.ssh_to(&ips[i]).await?;

            for (j, ip) in ips.iter().enumerate() {
                if j != i && j != prev && j != next {
                    ssh.exec(&format!("iptables -A INPUT -s {ip} -j DROP"))
                        .await?;
                    ssh.exec(&format!("iptables -A OUTPUT -d {ip} -j DROP"))
                        .await?;
                }
            }
        }

        tracing::info!(nodes = n, "Injected partition-ring");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("iptables -F INPUT").await?;
            ssh.exec("iptables -F OUTPUT").await?;
        }
        tracing::info!("Healed partition-ring");
        Ok(())
    }
}

/// Isolate a single random node from the rest of the cluster.
pub struct PartitionOne;

#[async_trait]
impl NemesisAction for PartitionOne {
    fn name(&self) -> &str {
        "partition-one"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        // Pre-compute the random choice before any await.
        let target = {
            let mut rng = rand::rng();
            ctx.node_ips.choose(&mut rng).cloned()
        };
        let target = target.expect("cluster must have at least one node");

        // Drop all traffic to/from the target on every other node.
        for ip in &ctx.node_ips {
            if ip == &target {
                continue;
            }
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec(&format!("iptables -A INPUT -s {target} -j DROP"))
                .await?;
            ssh.exec(&format!("iptables -A OUTPUT -d {target} -j DROP"))
                .await?;
        }

        // Also drop on the target itself.
        let ssh = ctx.ssh_to(&target).await?;
        for ip in &ctx.node_ips {
            if ip == &target {
                continue;
            }
            ssh.exec(&format!("iptables -A INPUT -s {ip} -j DROP"))
                .await?;
            ssh.exec(&format!("iptables -A OUTPUT -d {ip} -j DROP"))
                .await?;
        }

        tracing::info!(target = %target, "Injected partition-one");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("iptables -F INPUT").await?;
            ssh.exec("iptables -F OUTPUT").await?;
        }
        tracing::info!("Healed partition-one");
        Ok(())
    }
}

/// Add 100ms latency to all inter-node traffic using tc/netem.
pub struct SlowNetwork;

#[async_trait]
impl NemesisAction for SlowNetwork {
    fn name(&self) -> &str {
        "slow-network"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("tc qdisc add dev eth0 root netem delay 100ms")
                .await?;
        }
        tracing::info!("Injected slow-network (100ms delay) on all nodes");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("tc qdisc del dev eth0 root").await?;
        }
        tracing::info!("Healed slow-network");
        Ok(())
    }
}

/// Add random jitter (0-50ms) to inter-node traffic using tc/netem.
pub struct JitterNetwork;

#[async_trait]
impl NemesisAction for JitterNetwork {
    fn name(&self) -> &str {
        "jitter-network"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("tc qdisc add dev eth0 root netem delay 25ms 25ms")
                .await?;
        }
        tracing::info!("Injected jitter-network (25ms +/- 25ms) on all nodes");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("tc qdisc del dev eth0 root").await?;
        }
        tracing::info!("Healed jitter-network");
        Ok(())
    }
}

/// Drop 10% of packets randomly using tc/netem.
pub struct PacketLoss;

#[async_trait]
impl NemesisAction for PacketLoss {
    fn name(&self) -> &str {
        "packet-loss"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("tc qdisc add dev eth0 root netem loss 10%")
                .await?;
        }
        tracing::info!("Injected packet-loss (10%) on all nodes");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("tc qdisc del dev eth0 root").await?;
        }
        tracing::info!("Healed packet-loss");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_partition_halves() {
        assert_eq!(PartitionHalves.name(), "partition-halves");
    }

    #[test]
    fn name_partition_ring() {
        assert_eq!(PartitionRing.name(), "partition-ring");
    }

    #[test]
    fn name_partition_one() {
        assert_eq!(PartitionOne.name(), "partition-one");
    }

    #[test]
    fn name_slow_network() {
        assert_eq!(SlowNetwork.name(), "slow-network");
    }

    #[test]
    fn name_jitter_network() {
        assert_eq!(JitterNetwork.name(), "jitter-network");
    }

    #[test]
    fn name_packet_loss() {
        assert_eq!(PacketLoss.name(), "packet-loss");
    }
}
