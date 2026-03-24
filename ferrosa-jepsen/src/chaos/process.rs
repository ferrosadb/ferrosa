use anyhow::Result;
use async_trait::async_trait;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name() {
        assert_eq!(KillMinority.name(), "kill-minority");
    }
}
