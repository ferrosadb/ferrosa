use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use rand::seq::IndexedRandom;

use super::{
    machine_state_from_json, probe_says_serving, MachineAction, NemesisAction, NemesisContext,
    NemesisExecutor, SERVING_PROBE,
};

/// Polls for a SIGKILLed Fly machine to report `stopped` (1 s apart).
const STOP_POLLS: u32 = 30;
/// Polls for a restarted node to serve `/readyz` again (5 s apart, so 300 s). A node that never
/// rejoins silently turns the rest of the run into a smaller cluster, which is a weaker test, not
/// a pass, so running out of polls is an error.
const REJOIN_POLLS: u32 = 60;
const REJOIN_INTERVAL: Duration = Duration::from_secs(5);

/// Kill ferrosa process on minority nodes, restart after heal.
///
/// Over SSH this is `pkill -9` and a relaunch. On Fly it is a SIGKILL stop of each minority
/// machine through the Machines API, proven by the machine reaching `stopped`, and heal starts it
/// and waits until it serves `/readyz`. A container transport is refused loudly rather than
/// faked.
pub struct KillMinority;

#[async_trait]
impl NemesisAction for KillMinority {
    fn name(&self) -> &str {
        "kill-minority"
    }

    async fn inject(&self, ctx: &NemesisContext) -> Result<()> {
        match &ctx.executor {
            NemesisExecutor::Ssh => {
                for ip in &ctx.minority_ips() {
                    ctx.ssh_to(ip)
                        .await?
                        .exec("pkill -9 -f ferrosa || true")
                        .await?;
                }
            }
            NemesisExecutor::Fly { .. } => {
                for node in ctx.minority_indices() {
                    ctx.fly_machine(node, MachineAction::Kill).await?;
                    wait_for_stopped(ctx, node).await?;
                }
            }
            NemesisExecutor::Docker { .. } => {
                anyhow::bail!("kill-minority has no container transport; it would kill nothing")
            }
        }
        tracing::info!(nodes = ?ctx.minority_ips(), "Killed ferrosa on minority nodes");
        Ok(())
    }

    async fn heal(&self, ctx: &NemesisContext) -> Result<()> {
        match &ctx.executor {
            NemesisExecutor::Ssh => {
                let seed_arg = ctx.majority_ips().join(",");
                for ip in &ctx.minority_ips() {
                    ctx.ssh_to(ip)
                        .await?
                        .exec(&format!(
                            "nohup ferrosa --seeds {seed_arg} --listen {ip} > /var/log/ferrosa.log 2>&1 &"
                        ))
                        .await?;
                }
            }
            NemesisExecutor::Fly { .. } => {
                for node in ctx.minority_indices() {
                    ctx.fly_machine(node, MachineAction::Start).await?;
                    wait_until_serving(ctx, node).await?;
                }
            }
            NemesisExecutor::Docker { .. } => {
                anyhow::bail!("kill-minority has no container transport; it would restart nothing")
            }
        }
        tracing::info!(nodes = ?ctx.minority_ips(), "Restarted ferrosa on minority nodes");
        Ok(())
    }
}

/// Prove the kill bit: the machine must report `stopped` within [`STOP_POLLS`] seconds.
async fn wait_for_stopped(ctx: &NemesisContext, node: usize) -> Result<()> {
    let mut last = String::new();
    for _ in 0..STOP_POLLS {
        last = machine_state_from_json(&ctx.fly_machine(node, MachineAction::Status).await?)?;
        if last == "stopped" {
            tracing::info!(node, "kill-minority: machine is stopped");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    anyhow::bail!(
        "kill-minority: node {node} still `{last}` after {STOP_POLLS}s; the kill did not bite"
    )
}

/// Wait, bounded, until a restarted node serves `/readyz`. A transport error while the machine
/// boots is expected, so it is retried, but its first occurrence and the recovery are logged
/// (edges, not every poll) and the elapsed time is reported.
async fn wait_until_serving(ctx: &NemesisContext, node: usize) -> Result<()> {
    let started = Instant::now();
    let mut failing = false;
    for _ in 0..REJOIN_POLLS {
        match ctx.exec_on(node, SERVING_PROBE).await {
            Ok(out) if probe_says_serving(&out.stdout) => {
                tracing::info!(
                    node,
                    rejoin_seconds = started.elapsed().as_secs(),
                    "kill-minority: node serves /readyz again"
                );
                return Ok(());
            }
            Ok(_) if failing => {
                failing = false;
                tracing::info!(node, "kill-minority: probe reaches the node; not ready yet");
            }
            Ok(_) => {}
            Err(e) if !failing => {
                failing = true;
                tracing::warn!(node, error = %e, "kill-minority: readiness probe cannot reach the node yet");
            }
            Err(_) => {}
        }
        tokio::time::sleep(REJOIN_INTERVAL).await;
    }
    anyhow::bail!(
        "kill-minority: node {node} did not serve /readyz within {}s of restart",
        started.elapsed().as_secs()
    )
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
            let mut rng = rand::rng();
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
