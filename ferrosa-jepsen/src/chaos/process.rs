use anyhow::Result;
use async_trait::async_trait;
use rand::seq::SliceRandom;

use super::{NemesisAction, NemesisContext};

/// Kill ferrosa process on minority nodes, restart after heal.
pub struct KillMinority;

#[async_trait]
impl NemesisAction for KillMinority {
    fn name(&self) -> &str {
        "kill-minority"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        let minority = ctx.minority_ips();
        for ip in &minority {
            let ssh = ctx.ssh_to(ip).await?;
            // SIGKILL the ferrosa process.
            ssh.exec("pkill -9 -f ferrosa || true").await?;
        }
        tracing::info!(nodes = ?minority, "Killed ferrosa on minority nodes");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        let minority = ctx.minority_ips();
        let seeds = ctx.majority_ips();
        let seed_arg = seeds.join(",");

        for ip in &minority {
            let ssh = ctx.ssh_to(ip).await?;
            // Restart ferrosa with seed nodes.
            ssh.exec(&format!(
                "nohup ferrosa --seeds {seed_arg} --listen {ip} > /var/log/ferrosa.log 2>&1 &"
            ))
            .await?;
        }
        tracing::info!(nodes = ?minority, "Restarted ferrosa on minority nodes");
        Ok(())
    }
}

/// Kill ferrosa on majority nodes (cluster should lose quorum).
pub struct KillMajority;

#[async_trait]
impl NemesisAction for KillMajority {
    fn name(&self) -> &str {
        "kill-majority"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        let majority = ctx.majority_ips();
        for ip in &majority {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("pkill -9 -f ferrosa || true").await?;
        }
        tracing::info!(nodes = ?majority, "Killed ferrosa on majority nodes");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        let majority = ctx.majority_ips();
        let seeds = ctx.minority_ips();
        let seed_arg = if seeds.is_empty() {
            // If all nodes are majority (e.g. 1-node cluster), use first majority node.
            majority.first().cloned().unwrap_or_default()
        } else {
            seeds.join(",")
        };

        for ip in &majority {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec(&format!(
                "nohup ferrosa --seeds {seed_arg} --listen {ip} > /var/log/ferrosa.log 2>&1 &"
            ))
            .await?;
        }
        tracing::info!(nodes = ?majority, "Restarted ferrosa on majority nodes");
        Ok(())
    }
}

/// SIGSTOP a random node (pause, not kill).
pub struct PauseNode;

#[async_trait]
impl NemesisAction for PauseNode {
    fn name(&self) -> &str {
        "pause-node"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        // Pre-compute the random choice before any await.
        let target = {
            let mut rng = rand::thread_rng();
            ctx.node_ips.choose(&mut rng).cloned()
        };
        let target = target.expect("cluster must have at least one node");

        let ssh = ctx.ssh_to(&target).await?;
        ssh.exec("pkill -STOP -f ferrosa || true").await?;
        tracing::info!(node = %target, "Paused ferrosa (SIGSTOP)");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        // Resume ferrosa on all nodes (idempotent: SIGCONT on a running process is a no-op).
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("pkill -CONT -f ferrosa || true").await?;
        }
        tracing::info!("Healed pause-node (SIGCONT on all nodes)");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_kill_minority() {
        assert_eq!(KillMinority.name(), "kill-minority");
    }

    #[test]
    fn name_kill_majority() {
        assert_eq!(KillMajority.name(), "kill-majority");
    }

    #[test]
    fn name_pause_node() {
        assert_eq!(PauseNode.name(), "pause-node");
    }
}
