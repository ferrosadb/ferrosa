use anyhow::Result;
use async_trait::async_trait;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name() {
        assert_eq!(PartitionHalves.name(), "partition-halves");
    }
}
