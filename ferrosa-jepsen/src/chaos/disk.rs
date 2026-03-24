use anyhow::Result;
use async_trait::async_trait;

use super::{NemesisAction, NemesisContext};

/// Slow disk I/O by setting the ferrosa process to best-effort I/O class via ionice.
pub struct DiskSlow;

#[async_trait]
impl NemesisAction for DiskSlow {
    fn name(&self) -> &str {
        "disk-slow"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            // Set ferrosa process to idle I/O scheduling class (class 3).
            // This gives it the lowest I/O priority.
            ssh.exec("pgrep -f ferrosa | xargs -I{} ionice -c3 -p {} || true")
                .await?;
        }
        tracing::info!("Injected disk-slow (ionice class 3) on all nodes");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            // Restore ferrosa to best-effort I/O class 0 (normal priority).
            ssh.exec("pgrep -f ferrosa | xargs -I{} ionice -c0 -p {} || true")
                .await?;
        }
        tracing::info!("Healed disk-slow -- restored normal I/O priority");
        Ok(())
    }
}

/// Simulate disk failure using dm-flakey to make the underlying block device
/// drop all I/O for the configured interval.
pub struct DiskFail;

#[async_trait]
impl NemesisAction for DiskFail {
    fn name(&self) -> &str {
        "disk-fail"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            // Use dm-setup to create a flakey target that drops all I/O.
            // The target maps the entire device but with 0 up-time and
            // infinite down-time, effectively making it unresponsive.
            //
            // Assumes /dev/vda is the data disk and /dev/mapper/flakey is
            // the target name. The guest rootfs setup script configures this.
            ssh.exec(concat!(
                "SECTORS=$(blockdev --getsz /dev/vda) && ",
                "dmsetup create flakey --table ",
                "\"0 $SECTORS flakey /dev/vda 0 0 1\" || true"
            ))
            .await?;
        }
        tracing::info!("Injected disk-fail (dm-flakey) on all nodes");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            // Remove the dm-flakey mapping to restore normal disk access.
            ssh.exec("dmsetup remove flakey || true").await?;
        }
        tracing::info!("Healed disk-fail -- removed dm-flakey mapping");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_disk_slow() {
        assert_eq!(DiskSlow.name(), "disk-slow");
    }

    #[test]
    fn name_disk_fail() {
        assert_eq!(DiskFail.name(), "disk-fail");
    }
}
