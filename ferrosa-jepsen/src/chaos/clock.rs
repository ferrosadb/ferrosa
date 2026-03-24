use anyhow::Result;
use async_trait::async_trait;
use rand::Rng;

use super::{NemesisAction, NemesisContext};

/// Inject small clock skew (plus/minus 500ms) using libfaketime.
pub struct ClockSkewSmall;

#[async_trait]
impl NemesisAction for ClockSkewSmall {
    fn name(&self) -> &str {
        "clock-skew-small"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        // Pre-compute skews so the non-Send ThreadRng doesn't live across awaits.
        let skews: Vec<i64> = {
            let mut rng = rand::thread_rng();
            ctx.node_ips
                .iter()
                .map(|_| rng.gen_range(-500..=500))
                .collect()
        };

        for (ip, skew_ms) in ctx.node_ips.iter().zip(skews) {
            let skew_sign = if skew_ms >= 0 { "+" } else { "" };
            let skew_secs = skew_ms as f64 / 1000.0;

            let ssh = ctx.ssh_to(ip).await?;
            // Set FAKETIME environment for ferrosa process.
            ssh.exec(&format!(
                "echo 'FAKETIME=\"{skew_sign}{skew_secs}\"' > /etc/faketime.conf"
            ))
            .await?;
            // Kill the process so heal can restart without faketime.
            ssh.exec("pkill -f ferrosa || true").await?;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        tracing::info!("Injected clock-skew-small on all nodes");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        let seeds = ctx.node_ips.first().cloned().unwrap_or_default();

        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            // Remove faketime config.
            ssh.exec("rm -f /etc/faketime.conf").await?;
            // Restart without faketime.
            ssh.exec(&format!(
                "nohup ferrosa --seeds {seeds} --listen {ip} > /var/log/ferrosa.log 2>&1 &"
            ))
            .await?;
        }
        tracing::info!("Healed clock-skew-small — removed faketime, restarted nodes");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name() {
        assert_eq!(ClockSkewSmall.name(), "clock-skew-small");
    }
}
