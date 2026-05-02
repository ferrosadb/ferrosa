use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;

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
            let mut rng = rand::rng();
            ctx.node_ips
                .iter()
                .map(|_| rng.random_range(-500..=500))
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
        tracing::info!("Healed clock-skew-small -- removed faketime, restarted nodes");
        Ok(())
    }
}

/// Large clock skew (plus/minus 5 seconds) using libfaketime.
pub struct ClockSkewLarge;

#[async_trait]
impl NemesisAction for ClockSkewLarge {
    fn name(&self) -> &str {
        "clock-skew-large"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        // Pre-compute skews so the non-Send ThreadRng doesn't live across awaits.
        let skews: Vec<f64> = {
            let mut rng = rand::rng();
            ctx.node_ips
                .iter()
                .map(|_| {
                    let magnitude: f64 = rng.random_range(1.0..=5.0);
                    if rng.random_bool(0.5) {
                        magnitude
                    } else {
                        -magnitude
                    }
                })
                .collect()
        };

        for (ip, skew_secs) in ctx.node_ips.iter().zip(skews) {
            let skew_sign = if skew_secs >= 0.0 { "+" } else { "" };

            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec(&format!(
                "echo 'FAKETIME=\"{skew_sign}{skew_secs:.1}\"' > /etc/faketime.conf"
            ))
            .await?;
            ssh.exec("pkill -f ferrosa || true").await?;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        tracing::info!("Injected clock-skew-large on all nodes");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        let seeds = ctx.node_ips.first().cloned().unwrap_or_default();

        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("rm -f /etc/faketime.conf").await?;
            ssh.exec(&format!(
                "nohup ferrosa --seeds {seeds} --listen {ip} > /var/log/ferrosa.log 2>&1 &"
            ))
            .await?;
        }
        tracing::info!("Healed clock-skew-large -- removed faketime, restarted nodes");
        Ok(())
    }
}

/// Clock strobe: rapidly alternating time jumps (+2s/-2s every second).
///
/// Injects a background script on each node that toggles the FAKETIME offset
/// between +2s and -2s every second, creating rapid clock oscillation.
pub struct ClockStrobe;

#[async_trait]
impl NemesisAction for ClockStrobe {
    fn name(&self) -> &str {
        "clock-strobe"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        let strobe_script = r#"#!/bin/bash
while true; do
    echo 'FAKETIME="+2.0"' > /etc/faketime.conf
    sleep 1
    echo 'FAKETIME="-2.0"' > /etc/faketime.conf
    sleep 1
done"#;

        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            // Write the strobe script.
            ssh.exec(&format!(
                "cat > /tmp/clock_strobe.sh << 'SCRIPT'\n{strobe_script}\nSCRIPT"
            ))
            .await?;
            ssh.exec("chmod +x /tmp/clock_strobe.sh").await?;
            // Launch the strobe in the background.
            ssh.exec("nohup /tmp/clock_strobe.sh > /dev/null 2>&1 &")
                .await?;
        }
        tracing::info!("Injected clock-strobe on all nodes");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        let seeds = ctx.node_ips.first().cloned().unwrap_or_default();

        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            // Kill the strobe script.
            ssh.exec("pkill -f clock_strobe || true").await?;
            // Remove faketime config and script.
            ssh.exec("rm -f /etc/faketime.conf /tmp/clock_strobe.sh")
                .await?;
            // Restart ferrosa clean.
            ssh.exec(&format!(
                "nohup ferrosa --seeds {seeds} --listen {ip} > /var/log/ferrosa.log 2>&1 &"
            ))
            .await?;
        }
        tracing::info!("Healed clock-strobe -- killed strobe scripts, restarted nodes");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_clock_skew_small() {
        assert_eq!(ClockSkewSmall.name(), "clock-skew-small");
    }

    #[test]
    fn name_clock_skew_large() {
        assert_eq!(ClockSkewLarge.name(), "clock-skew-large");
    }

    #[test]
    fn name_clock_strobe() {
        assert_eq!(ClockStrobe.name(), "clock-strobe");
    }
}
