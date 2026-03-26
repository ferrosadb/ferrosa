use anyhow::Result;
use async_trait::async_trait;

use super::{NemesisAction, NemesisContext};

/// WAN bridge sidecar for injecting inter-DC failures.
pub struct WanBridge {
    pub bridge_ip: String,
    pub dc_a_ips: Vec<String>,
    pub dc_b_ips: Vec<String>,
}

/// Partition between two DCs -- all inter-DC traffic dropped.
pub struct DcPartition;

#[async_trait]
impl NemesisAction for DcPartition {
    fn name(&self) -> &str {
        "dc-partition"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        // Split nodes into DCs based on position.
        let mid = ctx.node_ips.len() / 2;
        let dc_a = &ctx.node_ips[..mid];
        let dc_b = &ctx.node_ips[mid..];

        // On each DC-A node, drop all DC-B traffic.
        for ip_a in dc_a {
            let ssh = ctx.ssh_to(ip_a).await?;
            for ip_b in dc_b {
                ssh.exec(&format!("iptables -A INPUT -s {} -j DROP", ip_b))
                    .await?;
                ssh.exec(&format!("iptables -A OUTPUT -d {} -j DROP", ip_b))
                    .await?;
            }
        }
        // And vice versa.
        for ip_b in dc_b {
            let ssh = ctx.ssh_to(ip_b).await?;
            for ip_a in dc_a {
                ssh.exec(&format!("iptables -A INPUT -s {} -j DROP", ip_a))
                    .await?;
                ssh.exec(&format!("iptables -A OUTPUT -d {} -j DROP", ip_a))
                    .await?;
            }
        }
        tracing::info!("Injected dc-partition between {:?} and {:?}", dc_a, dc_b);
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("iptables -F INPUT").await?;
            ssh.exec("iptables -F OUTPUT").await?;
        }
        tracing::info!("Healed dc-partition");
        Ok(())
    }
}

/// Add high latency (200ms) to inter-DC traffic.
pub struct DcSlow;

#[async_trait]
impl NemesisAction for DcSlow {
    fn name(&self) -> &str {
        "dc-slow"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        let mid = ctx.node_ips.len() / 2;
        let dc_b = &ctx.node_ips[mid..];
        // Add 200ms delay on DC-B nodes (simulates WAN latency).
        for ip in dc_b {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("tc qdisc add dev eth0 root netem delay 200ms 50ms")
                .await?;
        }
        tracing::info!("Injected dc-slow on DC-B nodes");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        let mid = ctx.node_ips.len() / 2;
        for ip in &ctx.node_ips[mid..] {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("tc qdisc del dev eth0 root 2>/dev/null || true")
                .await?;
        }
        tracing::info!("Healed dc-slow");
        Ok(())
    }
}

/// Asymmetric latency: DC-A -> DC-B has 300ms, DC-B -> DC-A has 50ms.
pub struct DcAsymmetric;

#[async_trait]
impl NemesisAction for DcAsymmetric {
    fn name(&self) -> &str {
        "dc-asymmetric"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        let mid = ctx.node_ips.len() / 2;
        // High latency on DC-A outbound.
        for ip in &ctx.node_ips[..mid] {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("tc qdisc add dev eth0 root netem delay 300ms")
                .await?;
        }
        // Low latency on DC-B outbound.
        for ip in &ctx.node_ips[mid..] {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("tc qdisc add dev eth0 root netem delay 50ms")
                .await?;
        }
        tracing::info!("Injected dc-asymmetric");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("tc qdisc del dev eth0 root 2>/dev/null || true")
                .await?;
        }
        Ok(())
    }
}

/// Flapping inter-DC connectivity: partition/heal every 2 seconds.
pub struct DcFlap;

#[async_trait]
impl NemesisAction for DcFlap {
    fn name(&self) -> &str {
        "dc-flap"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        // Start a background flapping script on first node.
        if let Some(ip) = ctx.node_ips.first() {
            let mid = ctx.node_ips.len() / 2;
            let remote_ips: Vec<&str> = ctx.node_ips[mid..].iter().map(|s| s.as_str()).collect();
            let remote_list = remote_ips.join(" ");

            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec(&format!(
                concat!(
                    "nohup sh -c 'while true; do ",
                    "for r in {}; do ",
                    "iptables -A INPUT -s $r -j DROP; ",
                    "iptables -A OUTPUT -d $r -j DROP; ",
                    "done; sleep 2; iptables -F INPUT; iptables -F OUTPUT; sleep 2; ",
                    "done' > /tmp/flap.log 2>&1 &"
                ),
                remote_list
            ))
            .await?;
        }
        tracing::info!("Injected dc-flap");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        for ip in &ctx.node_ips {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("pkill -f 'while true.*iptables' || true").await?;
            ssh.exec("iptables -F INPUT").await?;
            ssh.exec("iptables -F OUTPUT").await?;
        }
        tracing::info!("Healed dc-flap");
        Ok(())
    }
}

/// 5% packet loss on inter-DC links.
pub struct DcLossy;

#[async_trait]
impl NemesisAction for DcLossy {
    fn name(&self) -> &str {
        "dc-lossy"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        let mid = ctx.node_ips.len() / 2;
        for ip in &ctx.node_ips[mid..] {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("tc qdisc add dev eth0 root netem loss 5% corrupt 1%")
                .await?;
        }
        tracing::info!("Injected dc-lossy on DC-B");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        let mid = ctx.node_ips.len() / 2;
        for ip in &ctx.node_ips[mid..] {
            let ssh = ctx.ssh_to(ip).await?;
            ssh.exec("tc qdisc del dev eth0 root 2>/dev/null || true")
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_dc_partition() {
        assert_eq!(DcPartition.name(), "dc-partition");
    }

    #[test]
    fn name_dc_slow() {
        assert_eq!(DcSlow.name(), "dc-slow");
    }

    #[test]
    fn name_dc_asymmetric() {
        assert_eq!(DcAsymmetric.name(), "dc-asymmetric");
    }

    #[test]
    fn name_dc_flap() {
        assert_eq!(DcFlap.name(), "dc-flap");
    }

    #[test]
    fn name_dc_lossy() {
        assert_eq!(DcLossy.name(), "dc-lossy");
    }
}
