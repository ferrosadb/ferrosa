use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tracing::{debug, info, warn};

use crate::config::Topology;
use crate::firecracker::{FirecrackerVm, VmConfig};

/// A single node in a Ferrosa cluster.
pub struct ClusterNode {
    pub id: usize,
    pub ip: IpAddr,
    pub cql_port: u16,
    pub internode_port: u16,
    /// The backing Firecracker VM. `None` for Fly.io-backed nodes.
    pub vm: Option<FirecrackerVm>,
}

impl ClusterNode {
    /// Check if this node's CQL port is reachable via TCP connect.
    pub async fn cql_reachable(&self) -> bool {
        tokio::net::TcpStream::connect(self.cql_address())
            .await
            .is_ok()
    }

    /// Get the CQL contact string (`ip:port`) for this node.
    pub fn cql_address(&self) -> String {
        format!("{}:{}", self.ip, self.cql_port)
    }
}

/// A provisioned Ferrosa cluster backed by Firecracker microVMs.
pub struct FerrosCluster {
    pub topology: Topology,
    nodes: Vec<ClusterNode>,
    seed_ips: Vec<IpAddr>,
}

impl FerrosCluster {
    /// Provision a new cluster with the given topology.
    ///
    /// Creates one Firecracker VM per node, assigns sequential IPs starting
    /// from 172.16.0.2, and designates the first node as the seed.
    pub async fn provision(topology: Topology) -> Result<Self> {
        let node_count = topology.node_count();
        let base_ip: [u8; 4] = [172, 16, 0, 2];

        let mut nodes = Vec::with_capacity(node_count);
        let mut seed_ips = Vec::new();

        for i in 0..node_count {
            let last_octet = base_ip[3]
                .checked_add(i as u8)
                .context("too many nodes — IP octet overflow")?;
            let ip_bytes = [base_ip[0], base_ip[1], base_ip[2], last_octet];
            let ip: IpAddr = ip_bytes.into();

            info!(node = i, %ip, "provisioning VM");

            let vm_config = VmConfig {
                vcpu: 2,
                mem_mb: 1024,
                rootfs: PathBuf::from("rootfs/ferrosa.ext4"),
                kernel: PathBuf::from("rootfs/vmlinux"),
                tap_device: format!("tap{i}"),
                ip: ip.to_string(),
                gateway: "172.16.0.1".into(),
                socket_path: PathBuf::from(format!("/tmp/ferrosa-jepsen-vm-{i}.sock")),
            };

            let vm = FirecrackerVm::create(vm_config).await?;

            if i == 0 {
                seed_ips.push(ip);
            }

            nodes.push(ClusterNode {
                id: i,
                ip,
                cql_port: 9042,
                internode_port: 7000,
                vm: Some(vm),
            });
        }

        // TODO: SSH into each node, run setup-guest.sh, start ferrosa.
        // The first node is the seed; subsequent nodes join via the seed IP.

        info!(
            node_count = nodes.len(),
            seed = %seed_ips.first().map(|ip| ip.to_string()).unwrap_or_default(),
            "cluster provisioned"
        );

        Ok(Self {
            topology,
            nodes,
            seed_ips,
        })
    }

    /// Wait for all nodes to be ready (CQL port responding) or time out.
    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        let poll_interval = Duration::from_millis(500);

        for node in &self.nodes {
            debug!(node = node.id, %node.ip, "waiting for CQL readiness");

            loop {
                if node.cql_reachable().await {
                    info!(node = node.id, %node.ip, "CQL ready");
                    break;
                }
                if tokio::time::Instant::now() > deadline {
                    bail!(
                        "node {} ({}) did not become CQL-ready within {:?}",
                        node.id,
                        node.ip,
                        timeout,
                    );
                }
                tokio::time::sleep(poll_interval).await;
            }
        }

        Ok(())
    }

    /// Get all nodes in the cluster.
    pub fn nodes(&self) -> &[ClusterNode] {
        &self.nodes
    }

    /// Get the seed IPs for this cluster.
    pub fn seed_ips(&self) -> &[IpAddr] {
        &self.seed_ips
    }

    /// Get a random CQL contact point (`ip:port`).
    pub fn random_contact_point(&self) -> String {
        let idx = rand::RngExt::random_range(&mut rand::rng(), 0..self.nodes.len());
        self.nodes[idx].cql_address()
    }

    /// Connect to a pre-existing cluster using the given CQL contact addresses.
    ///
    /// Each entry in `nodes` should be a "host:port" string.
    /// Returns a `FerrosCluster` with no associated VMs (vm = None).
    pub async fn from_nodes(nodes: &[String]) -> Result<Self> {
        let cluster_nodes: Vec<ClusterNode> = nodes
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let ip: IpAddr = addr
                    .rsplit_once(':')
                    .and_then(|(host, _)| host.parse().ok())
                    .unwrap_or_else(|| "127.0.0.1".parse().unwrap());
                let cql_port: u16 = addr
                    .rsplit_once(':')
                    .and_then(|(_, port)| port.parse().ok())
                    .unwrap_or(9042);
                ClusterNode {
                    id: i,
                    ip,
                    cql_port,
                    internode_port: 7000,
                    vm: None,
                }
            })
            .collect();

        let seed_ips = cluster_nodes
            .first()
            .map(|n| vec![n.ip])
            .unwrap_or_default();

        Ok(Self {
            topology: Topology::T1,
            nodes: cluster_nodes,
            seed_ips,
        })
    }

    /// Teardown the cluster by destroying all backing VMs.
    pub async fn teardown(mut self) -> Result<()> {
        info!("tearing down cluster");

        for node in &mut self.nodes {
            if let Some(mut vm) = node.vm.take() {
                if let Err(e) = vm.destroy().await {
                    warn!(node = node.id, error = %e, "failed to destroy VM");
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_node_address() {
        let node = ClusterNode {
            id: 0,
            ip: "172.16.0.2".parse().unwrap(),
            cql_port: 9042,
            internode_port: 7000,
            vm: None,
        };
        assert_eq!(node.cql_address(), "172.16.0.2:9042");
    }

    #[cfg(feature = "live-infra-tests")]
    #[tokio::test]
    async fn provision_t1_cluster() {
        if std::env::var("FERROSA_TEST_CLUSTER_NODES").is_err()
            && std::env::var("FERROSA_TEST_FIRECRACKER").is_err()
        {
            panic!(
                "cluster infrastructure not available — set FERROSA_TEST_CLUSTER_NODES \
                 or run scripts/lima-fc-cluster-up.sh and set FERROSA_TEST_FIRECRACKER=1"
            );
        }
        // Firecracker only runs on Linux — this test must execute from inside Lima.
        if std::env::var("FERROSA_TEST_FIRECRACKER").is_ok()
            && std::process::Command::new("which")
                .arg("firecracker")
                .output()
                .map(|o| !o.status.success())
                .unwrap_or(true)
        {
            panic!(
                "firecracker binary not found in PATH — this test must run from inside the Lima VM\n\
                 Run: limactl shell mvm\n\
                 Then: FERROSA_TEST_FIRECRACKER=1 cargo test -p ferrosa-jepsen provision_t1_cluster"
            );
        }
        let cluster = FerrosCluster::provision(Topology::T1).await.unwrap();
        assert_eq!(cluster.nodes().len(), 3);
        cluster.wait_ready(Duration::from_secs(60)).await.unwrap();
        for node in cluster.nodes() {
            assert!(node.cql_reachable().await);
        }
        cluster.teardown().await.unwrap();
    }
}
