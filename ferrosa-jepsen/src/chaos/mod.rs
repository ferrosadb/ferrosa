pub mod clock;
pub mod composed;
pub mod disk;
pub mod network;
pub mod process;
pub mod topology;
pub mod wan_bridge;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Host-facing admin endpoint of the local single-DC Jepsen cluster's first
/// node (`jepsen-cluster.yml` node1), used by nemeses that read cluster
/// membership. Its host port must stay below the kernel's ephemeral range floor
/// (32768) — an outbound connection can otherwise already hold it as a source
/// port and `docker compose up` fails to publish the mapping. The
/// `host_port_band` test pins this.
pub const DEFAULT_ADMIN_URL: &str = "http://localhost:29090/admin/membership-snapshot";

/// A nemesis action that can be injected and healed.
#[async_trait]
pub trait NemesisAction: Send + Sync {
    /// Human-readable name of this nemesis.
    fn name(&self) -> &str;

    /// Inject the failure.
    async fn inject(&self, ctx: &NemesisContext) -> Result<()>;

    /// Heal/recover from the failure.
    async fn heal(&self, ctx: &NemesisContext) -> Result<()>;
}

/// Context provided to nemesis actions -- node IPs, SSH access, etc.
pub struct NemesisContext {
    /// All node IPs in the cluster.
    pub node_ips: Vec<String>,
    /// SSH user for connecting to nodes.
    pub ssh_user: String,
    /// SSH key path.
    pub ssh_key_path: PathBuf,
    /// SSH port.
    pub ssh_port: u16,
    /// How chaos commands reach nodes. Fly's managed SSH transport is used
    /// for real multi-DC machines; local Firecracker tests use direct SSH.
    pub executor: NemesisExecutor,
}

/// Transport used to execute a nemesis command on a particular node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NemesisExecutor {
    Ssh,
    Docker {
        container_names: Vec<String>,
    },
    Fly {
        app_name: String,
        machine_ids: Vec<String>,
    },
}

impl NemesisContext {
    /// Build a Fly-backed context. Machine order must match `node_ips`, so the
    /// first half remains DC-A and the second half DC-B for WAN nemeses.
    pub fn fly(
        node_ips: Vec<String>,
        app_name: impl Into<String>,
        machine_ids: Vec<String>,
    ) -> Result<Self> {
        if node_ips.len() != machine_ids.len() {
            anyhow::bail!(
                "Fly nemesis needs one machine ID per node: {} nodes, {} machine IDs",
                node_ips.len(),
                machine_ids.len()
            );
        }
        Ok(Self {
            node_ips,
            ssh_user: "root".to_string(),
            ssh_key_path: PathBuf::new(),
            ssh_port: 22,
            executor: NemesisExecutor::Fly {
                app_name: app_name.into(),
                machine_ids,
            },
        })
    }

    /// The Fly app and the machine ID of one ordered node.
    fn fly_target(&self, node_index: usize) -> Result<(&str, &str)> {
        let NemesisExecutor::Fly {
            app_name,
            machine_ids,
        } = &self.executor
        else {
            anyhow::bail!("Fly command requested for a non-Fly nemesis context");
        };
        let machine_id = machine_ids
            .get(node_index)
            .ok_or_else(|| anyhow::anyhow!("missing Fly machine ID for node index {node_index}"))?;
        Ok((app_name, machine_id))
    }

    fn fly_ssh_args(&self, node_index: usize, command: &str) -> Result<Vec<String>> {
        let (app_name, machine_id) = self.fly_target(node_index)?;
        let quoted = command.replace('\'', "'\"'\"'");
        Ok(vec![
            "ssh".to_string(),
            "console".to_string(),
            "--app".to_string(),
            app_name.to_string(),
            "--machine".to_string(),
            machine_id.to_string(),
            "--command".to_string(),
            format!("sh -lc '{quoted}'"),
        ])
    }

    /// `fly` arguments for a Machines API action on one ordered node. A kill is a SIGKILL stop,
    /// not a graceful one: a clean shutdown could flush state a real crash would not.
    fn fly_machine_args(&self, node_index: usize, action: MachineAction) -> Result<Vec<String>> {
        let (app_name, machine_id) = self.fly_target(node_index)?;
        let mut args: Vec<String> = ["machine", action.verb(), machine_id, "--app", app_name]
            .into_iter()
            .map(str::to_string)
            .collect();
        match action {
            MachineAction::Kill => args.extend(["--signal".to_string(), "SIGKILL".to_string()]),
            MachineAction::Status => args.push("--json".to_string()),
            MachineAction::Start => {}
        }
        Ok(args)
    }

    /// Run a Machines API action from this host (the guest cannot stop itself observably). Fails
    /// loud on a non-zero exit; returns stdout.
    pub(crate) async fn fly_machine(
        &self,
        node_index: usize,
        action: MachineAction,
    ) -> Result<String> {
        let args = self.fly_machine_args(node_index, action)?;
        let output = tokio::process::Command::new("fly")
            .args(&args)
            .output()
            .await?;
        if !output.status.success() {
            anyhow::bail!(
                "`fly {}` failed with {:?}: {}",
                args.join(" "),
                output.status.code(),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Execute a command on one ordered cluster node through the configured
    /// transport. WAN nemeses use this rather than assuming direct SSH.
    pub async fn exec_on(
        &self,
        node_index: usize,
        command: &str,
    ) -> Result<crate::ssh::CommandOutput> {
        match &self.executor {
            NemesisExecutor::Ssh => {
                let ip = self
                    .node_ips
                    .get(node_index)
                    .ok_or_else(|| anyhow::anyhow!("missing node IP for index {node_index}"))?;
                self.ssh_to(ip).await?.exec(command).await
            }
            NemesisExecutor::Fly { .. } => {
                let args = self.fly_ssh_args(node_index, command)?;
                let output = tokio::process::Command::new("fly")
                    .args(args)
                    .output()
                    .await?;
                let status = output.status.code().unwrap_or(255);
                let result = crate::ssh::CommandOutput {
                    stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                    stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                    exit_code: status,
                };
                if output.status.success() {
                    Ok(result)
                } else {
                    anyhow::bail!(
                        "Fly command on node {node_index} failed with {status}: {}",
                        result.stderr
                    );
                }
            }
            NemesisExecutor::Docker { container_names } => {
                let container = container_names.get(node_index).ok_or_else(|| {
                    anyhow::anyhow!("missing Docker container for node index {node_index}")
                })?;
                let output =
                    tokio::process::Command::new(crate::docker_provision::container_runtime())
                        .args(["exec", container, "sh", "-lc", command])
                        .output()
                        .await?;
                Ok(crate::ssh::CommandOutput {
                    stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                    stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                    exit_code: output.status.code().unwrap_or(255),
                })
            }
        }
    }

    /// Indices of the minority nodes (the last `n - (n/2 + 1)`), matching [`Self::minority_ips`].
    pub fn minority_indices(&self) -> std::ops::Range<usize> {
        (self.node_ips.len() / 2) + 1..self.node_ips.len()
    }

    /// Get IPs for "majority" partition (more than half).
    pub fn majority_ips(&self) -> Vec<String> {
        let n = (self.node_ips.len() / 2) + 1;
        self.node_ips[..n].to_vec()
    }

    /// Get IPs for "minority" partition (less than half).
    pub fn minority_ips(&self) -> Vec<String> {
        let n = (self.node_ips.len() / 2) + 1;
        self.node_ips[n..].to_vec()
    }

    /// Connect SSH to a specific node IP.
    pub async fn ssh_to(&self, ip: &str) -> Result<crate::ssh::SshClient> {
        crate::ssh::SshClient::connect(ip, self.ssh_port, &self.ssh_user, &self.ssh_key_path).await
    }
}

/// A Machines API action a Fly nemesis takes against one node from outside the guest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MachineAction {
    /// `fly machine stop --signal SIGKILL`: process death without a clean shutdown.
    Kill,
    /// `fly machine start`.
    Start,
    /// `fly machine status --json`.
    Status,
}

impl MachineAction {
    fn verb(self) -> &'static str {
        match self {
            MachineAction::Kill => "stop",
            MachineAction::Start => "start",
            MachineAction::Status => "status",
        }
    }
}

/// The `state` field of `fly machine status --json`. A document without one is an error, never
/// an assumed state.
pub(crate) fn machine_state_from_json(json: &str) -> Result<String> {
    let value: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| anyhow::anyhow!("machine status is not JSON ({e}): {json}"))?;
    value
        .get("state")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("machine status has no `state`: {json}"))
}

/// Shell probe that prints `UP` when the node's own `/readyz` answers and `DOWN` otherwise. It
/// always exits 0, so a transport failure and a node that is not ready stay distinguishable.
pub(crate) const SERVING_PROBE: &str =
    "curl -sf --max-time 3 http://localhost:9090/readyz >/dev/null && echo UP || echo DOWN";

/// Whether [`SERVING_PROBE`] output says the node serves. Only an exact `UP` line counts.
pub(crate) fn probe_says_serving(stdout: &str) -> bool {
    stdout.lines().any(|line| line.trim() == "UP")
}

/// One shell command for one node, by its index in [`NemesisContext::node_ips`].
pub type NodeCommand = (usize, String);

/// Select the netfilter family for a peer address. Docker bridges use IPv4, while Fly private
/// addresses are IPv6; invoking `iptables` for the latter either rejects the address or leaves
/// the path untouched.
pub(crate) fn iptables_binary_for(address: &str) -> &'static str {
    if address.contains(':') {
        "ip6tables"
    } else {
        "iptables"
    }
}

/// Execute a chaos command through the context's configured transport (SSH, Fly SSH or a
/// container runtime) and fail loudly if the node rejects it, so a fault that did not apply never
/// reads as one that did.
pub(crate) async fn exec_required(
    ctx: &NemesisContext,
    node_index: usize,
    command: &str,
) -> Result<()> {
    let output = ctx.exec_on(node_index, command).await?;
    if output.exit_code != 0 {
        anyhow::bail!(
            "chaos command `{command}` failed on node {node_index} with {}: {}",
            output.exit_code,
            output.stderr
        );
    }
    Ok(())
}

/// Run every command of a plan, in order, failing on the first that does not apply.
pub(crate) async fn run_plan(ctx: &NemesisContext, plan: &[NodeCommand]) -> Result<()> {
    for (node_index, command) in plan {
        exec_required(ctx, *node_index, command).await?;
    }
    Ok(())
}

/// Commands that cut every node in `isolated` off from every other node, in both directions and
/// on both ends, with the netfilter family of each peer address.
pub fn isolate_plan(node_ips: &[String], isolated: &[usize]) -> Vec<NodeCommand> {
    let mut plan = Vec::new();
    let others: Vec<usize> = (0..node_ips.len())
        .filter(|i| !isolated.contains(i))
        .collect();
    for (side, peers) in [(isolated, others.as_slice()), (others.as_slice(), isolated)] {
        for &node in side {
            for &peer in peers {
                let ip = &node_ips[peer];
                let iptables = iptables_binary_for(ip);
                plan.push((node, format!("{iptables} -A INPUT -s {ip} -j DROP")));
                plan.push((node, format!("{iptables} -A OUTPUT -d {ip} -j DROP")));
            }
        }
    }
    plan
}

/// Commands that flush the INPUT and OUTPUT chains on every node, in each node's own family.
pub fn flush_plan(node_ips: &[String]) -> Vec<NodeCommand> {
    node_ips
        .iter()
        .enumerate()
        .flat_map(|(node, ip)| {
            let iptables = iptables_binary_for(ip);
            [
                (node, format!("{iptables} -F INPUT")),
                (node, format!("{iptables} -F OUTPUT")),
            ]
        })
        .collect()
}

/// Schedule for running nemeses during a test.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NemesisSchedule {
    /// Name of nemesis to use.
    pub nemesis_name: String,
    /// How long the failure lasts.
    pub inject_duration: Duration,
    /// How long between injections.
    pub heal_duration: Duration,
    /// Total number of inject/heal cycles.
    pub cycles: usize,
}

/// No-op nemesis — injects and heals without doing anything.
///
/// Used as a baseline: a healthy cluster with no fault injection must pass
/// linearizability checks. If it doesn't, the workload or checker is broken.
pub struct NoOp;

#[async_trait]
impl NemesisAction for NoOp {
    fn name(&self) -> &str {
        "noop"
    }

    async fn inject(&self, _ctx: &NemesisContext) -> Result<()> {
        tracing::debug!("noop nemesis: inject (nothing to do)");
        Ok(())
    }

    async fn heal(&self, _ctx: &NemesisContext) -> Result<()> {
        tracing::debug!("noop nemesis: heal (nothing to do)");
        Ok(())
    }
}

/// Registry of available nemeses.
pub struct NemesisRegistry {
    nemeses: std::collections::HashMap<String, Box<dyn NemesisAction>>,
}

impl Default for NemesisRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl NemesisRegistry {
    pub fn new() -> Self {
        Self {
            nemeses: std::collections::HashMap::new(),
        }
    }

    pub fn register(&mut self, nemesis: Box<dyn NemesisAction>) {
        let name = nemesis.name().to_string();
        self.nemeses.insert(name, nemesis);
    }

    pub fn get(&self, name: &str) -> Option<&dyn NemesisAction> {
        self.nemeses.get(name).map(|b| b.as_ref())
    }

    pub fn names(&self) -> Vec<String> {
        self.nemeses.keys().cloned().collect()
    }

    /// Create a registry with all Phase 1 nemeses pre-registered.
    pub fn phase1() -> Self {
        let mut reg = Self::new();
        reg.register(Box::new(NoOp));
        reg.register(Box::new(network::PartitionHalves));
        reg.register(Box::new(process::KillMinority));
        reg.register(Box::new(clock::ClockSkewSmall));
        // Sprint 2 topology nemeses (W2.6/W2.7/W2.8). They are in the
        // smoke tier because they exercise the membership bug class
        // every PR must regress against.
        reg.register(Box::new(topology::AddNodeViaFollower {
            seed_service: "node3".to_string(),
            new_node_service: "node4".to_string(),
            compose_file: None,
        }));
        reg.register(Box::new(topology::DecommissionLeader {
            admin_url: DEFAULT_ADMIN_URL.to_string(),
            ctl_binary: "ferrosa-ctl".to_string(),
        }));
        reg.register(Box::new(topology::RandomStartupOrder {
            compose_file: None,
            node_services: vec![
                "node1".to_string(),
                "node2".to_string(),
                "node3".to_string(),
            ],
        }));
        reg
    }

    /// Create a registry with all 16 Phase 2 nemeses pre-registered.
    pub fn phase2() -> Self {
        let mut reg = Self::phase1();
        // Network (5 more)
        reg.register(Box::new(network::PartitionRing));
        reg.register(Box::new(network::PartitionOne));
        reg.register(Box::new(network::SlowNetwork));
        reg.register(Box::new(network::JitterNetwork));
        reg.register(Box::new(network::PacketLoss));
        // Process (2 more)
        reg.register(Box::new(process::KillMajority));
        reg.register(Box::new(process::PauseNode));
        // Clock (2 more)
        reg.register(Box::new(clock::ClockSkewLarge));
        reg.register(Box::new(clock::ClockStrobe));
        // Disk (2)
        reg.register(Box::new(disk::DiskSlow));
        reg.register(Box::new(disk::DiskFail));
        reg
    }

    /// Phase 4: all nemeses including WAN and composed.
    pub fn full() -> Self {
        let mut reg = Self::phase2();
        // WAN nemeses (5)
        reg.register(Box::new(wan_bridge::DcPartition));
        reg.register(Box::new(wan_bridge::DcSlow));
        reg.register(Box::new(wan_bridge::DcAsymmetric));
        reg.register(Box::new(wan_bridge::DcFlap));
        reg.register(Box::new(wan_bridge::DcLossy));
        // Composed (6) — Sprint 7 W7.11 adds dc-partition+dc-slow
        reg.register(Box::new(composed::partition_and_kill()));
        reg.register(Box::new(composed::slow_and_clock()));
        reg.register(Box::new(composed::dc_partition_and_kill()));
        reg.register(Box::new(composed::dc_slow_and_disk()));
        reg.register(Box::new(composed::dc_partition_and_slow()));
        reg.register(Box::new(composed::everything()));
        reg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nemesis_registry_phase1() {
        let reg = NemesisRegistry::phase1();
        let mut names = reg.names();
        names.sort();
        // Sprint 2: phase1 now also includes the three topology nemeses.
        assert_eq!(
            names,
            vec![
                "add-node-via-follower",
                "clock-skew-small",
                "decommission-leader",
                "kill-minority",
                "noop",
                "partition-halves",
                "random-startup-order",
            ]
        );
    }

    #[test]
    fn nemesis_registry_phase2() {
        let reg = NemesisRegistry::phase2();
        let mut names = reg.names();
        names.sort();
        // 15 phase2 + 3 Sprint 2 topology = 18.
        assert_eq!(names.len(), 18);
        assert_eq!(
            names,
            vec![
                "add-node-via-follower",
                "clock-skew-large",
                "clock-skew-small",
                "clock-strobe",
                "decommission-leader",
                "disk-fail",
                "disk-slow",
                "jitter-network",
                "kill-majority",
                "kill-minority",
                "noop",
                "packet-loss",
                "partition-halves",
                "partition-one",
                "partition-ring",
                "pause-node",
                "random-startup-order",
                "slow-network",
            ]
        );
    }

    #[test]
    fn nemesis_registry_full() {
        let reg = NemesisRegistry::full();
        let names = reg.names();
        assert!(names.len() >= 25); // 15 phase2 + 5 WAN + 5 composed
        assert!(names.contains(&"dc-partition".to_string()));
        assert!(names.contains(&"partition+kill".to_string()));
    }

    #[test]
    fn nemesis_context_partitioning() {
        let ctx = NemesisContext {
            node_ips: vec!["1".into(), "2".into(), "3".into()],
            ssh_user: "root".into(),
            ssh_key_path: PathBuf::from("/tmp/key"),
            ssh_port: 22,
            executor: NemesisExecutor::Ssh,
        };
        assert_eq!(ctx.majority_ips(), vec!["1", "2"]);
        assert_eq!(ctx.minority_ips(), vec!["3"]);
    }

    #[test]
    fn nemesis_context_5_node_partitioning() {
        let ctx = NemesisContext {
            node_ips: vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into()],
            ssh_user: "root".into(),
            ssh_key_path: PathBuf::from("/tmp/key"),
            ssh_port: 22,
            executor: NemesisExecutor::Ssh,
        };
        assert_eq!(ctx.majority_ips().len(), 3);
        assert_eq!(ctx.minority_ips().len(), 2);
    }

    #[test]
    fn fly_context_targets_the_matching_machine_with_shell_quoting() {
        let ctx = NemesisContext::fly(
            vec!["fdaa:0:1::1".into(), "fdaa:0:1::2".into()],
            "ferrosa-jepsen-live",
            vec!["machine-a".into(), "machine-b".into()],
        )
        .expect("one Fly machine ID per node");

        assert_eq!(
            ctx.fly_ssh_args(1, "iptables -A OUTPUT -d 'fdaa:0:1::1' -j DROP")
                .expect("Fly command"),
            vec![
                "ssh",
                "console",
                "--app",
                "ferrosa-jepsen-live",
                "--machine",
                "machine-b",
                "--command",
                "sh -lc 'iptables -A OUTPUT -d '\"'\"'fdaa:0:1::1'\"'\"' -j DROP'",
            ],
            "the WAN action must execute on the selected Fly machine, not the local runner"
        );
    }

    fn fly_ips() -> Vec<String> {
        vec![
            "fdaa:0:1::1".into(),
            "fdaa:0:1::2".into(),
            "fdaa:0:1::3".into(),
        ]
    }

    #[test]
    fn isolating_the_minority_drops_both_directions_on_both_ends_in_ip6tables() {
        let plan = isolate_plan(&fly_ips(), &[2]);
        let expected: Vec<NodeCommand> = vec![
            (2, "ip6tables -A INPUT -s fdaa:0:1::1 -j DROP".into()),
            (2, "ip6tables -A OUTPUT -d fdaa:0:1::1 -j DROP".into()),
            (2, "ip6tables -A INPUT -s fdaa:0:1::2 -j DROP".into()),
            (2, "ip6tables -A OUTPUT -d fdaa:0:1::2 -j DROP".into()),
            (0, "ip6tables -A INPUT -s fdaa:0:1::3 -j DROP".into()),
            (0, "ip6tables -A OUTPUT -d fdaa:0:1::3 -j DROP".into()),
            (1, "ip6tables -A INPUT -s fdaa:0:1::3 -j DROP".into()),
            (1, "ip6tables -A OUTPUT -d fdaa:0:1::3 -j DROP".into()),
        ];
        assert_eq!(plan, expected);
    }

    #[test]
    fn isolating_an_ipv4_node_uses_iptables_and_never_touches_the_majority_link() {
        let ips: Vec<String> = vec!["10.0.0.1".into(), "10.0.0.2".into(), "10.0.0.3".into()];
        let plan = isolate_plan(&ips, &[2]);
        assert_eq!(plan.len(), 8);
        assert!(plan.iter().all(|(_, c)| c.starts_with("iptables ")));
        assert!(
            !plan
                .iter()
                .any(|(node, c)| *node == 0 && c.contains("10.0.0.2")),
            "the majority must keep talking to itself: {plan:?}"
        );
    }

    #[test]
    fn healing_flushes_every_node_in_its_own_family() {
        let ips: Vec<String> = vec!["10.0.0.1".into(), "fdaa:0:1::2".into()];
        assert_eq!(
            flush_plan(&ips),
            vec![
                (0, "iptables -F INPUT".to_string()),
                (0, "iptables -F OUTPUT".to_string()),
                (1, "ip6tables -F INPUT".to_string()),
                (1, "ip6tables -F OUTPUT".to_string()),
            ]
        );
    }

    #[test]
    fn minority_indices_match_minority_ips() {
        let ctx = NemesisContext::fly(
            fly_ips(),
            "app",
            vec!["m1".into(), "m2".into(), "m3".into()],
        )
        .expect("context");
        assert_eq!(ctx.minority_indices(), 2..3);
        assert_eq!(ctx.minority_ips(), vec!["fdaa:0:1::3".to_string()]);
    }

    #[test]
    fn a_fly_kill_is_a_sigkill_stop_of_the_matching_machine_and_a_start_brings_it_back() {
        let ctx = NemesisContext::fly(
            fly_ips(),
            "ferrosa-je6",
            vec!["m1".into(), "m2".into(), "m3".into()],
        )
        .expect("context");
        assert_eq!(
            ctx.fly_machine_args(2, MachineAction::Kill)
                .expect("kill args"),
            vec![
                "machine",
                "stop",
                "m3",
                "--app",
                "ferrosa-je6",
                "--signal",
                "SIGKILL"
            ]
        );
        assert_eq!(
            ctx.fly_machine_args(2, MachineAction::Start)
                .expect("start args"),
            vec!["machine", "start", "m3", "--app", "ferrosa-je6"]
        );
        assert_eq!(
            ctx.fly_machine_args(0, MachineAction::Status)
                .expect("status args"),
            vec!["machine", "status", "m1", "--app", "ferrosa-je6", "--json"]
        );
        assert!(ctx.fly_machine_args(3, MachineAction::Kill).is_err());
    }

    #[test]
    fn machine_state_is_read_from_the_status_json_and_a_missing_state_is_an_error() {
        assert_eq!(
            machine_state_from_json(r#"{"id":"m3","state":"stopped","name":"ferrosa-3"}"#)
                .expect("state"),
            "stopped"
        );
        assert!(machine_state_from_json(r#"{"id":"m3"}"#).is_err());
        assert!(machine_state_from_json("not json").is_err());
    }

    #[test]
    fn only_an_up_line_counts_as_serving() {
        assert!(probe_says_serving("Connecting to fdaa...\nUP\n"));
        assert!(!probe_says_serving("DOWN\n"));
        assert!(!probe_says_serving(""));
        assert!(!probe_says_serving("UPGRADE\n"));
    }

    #[test]
    fn nemesis_schedule_serializes() {
        let sched = NemesisSchedule {
            nemesis_name: "partition-halves".to_string(),
            inject_duration: Duration::from_secs(10),
            heal_duration: Duration::from_secs(30),
            cycles: 5,
        };
        let json = serde_json::to_string(&sched).unwrap();
        let back: NemesisSchedule = serde_json::from_str(&json).unwrap();
        assert_eq!(back.nemesis_name, "partition-halves");
        assert_eq!(back.cycles, 5);
        assert_eq!(back.inject_duration, Duration::from_secs(10));
    }

    #[test]
    fn noop_nemesis_has_correct_name() {
        let noop = NoOp;
        assert_eq!(noop.name(), "noop");
    }

    #[test]
    fn all_phase1_nemeses_have_unique_names() {
        let reg = NemesisRegistry::phase1();
        let names = reg.names();
        let set: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(names.len(), set.len(), "duplicate nemesis names in phase1");
    }

    #[test]
    fn all_full_nemeses_have_unique_names() {
        let reg = NemesisRegistry::full();
        let names = reg.names();
        let set: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(
            names.len(),
            set.len(),
            "duplicate nemesis names in full registry"
        );
    }

    #[test]
    fn registry_get_returns_registered_nemesis() {
        let reg = NemesisRegistry::phase1();
        assert!(reg.get("noop").is_some());
        assert!(reg.get("partition-halves").is_some());
        assert!(reg.get("nonexistent").is_none());
    }

    #[test]
    fn individual_nemesis_names() {
        assert_eq!(network::PartitionHalves.name(), "partition-halves");
        assert_eq!(network::PartitionRing.name(), "partition-ring");
        assert_eq!(network::PartitionOne.name(), "partition-one");
        assert_eq!(network::SlowNetwork.name(), "slow-network");
        assert_eq!(network::JitterNetwork.name(), "jitter-network");
        assert_eq!(network::PacketLoss.name(), "packet-loss");
        assert_eq!(process::KillMinority.name(), "kill-minority");
        assert_eq!(process::KillMajority.name(), "kill-majority");
        assert_eq!(process::PauseNode.name(), "pause-node");
        assert_eq!(clock::ClockSkewSmall.name(), "clock-skew-small");
        assert_eq!(clock::ClockSkewLarge.name(), "clock-skew-large");
        assert_eq!(clock::ClockStrobe.name(), "clock-strobe");
        assert_eq!(disk::DiskSlow.name(), "disk-slow");
        assert_eq!(disk::DiskFail.name(), "disk-fail");
    }
}
