//! Docker Compose cluster provisioning for ferrosa-jepsen.
//!
//! Starts and tears down a 3-node Ferrosa cluster using Docker Compose.
//! The compose file lives at `ferrosa-jepsen/tests/docker/jepsen-cluster.yml`
//! relative to the workspace root.
//!
//! # Environment
//!
//! Tests that call these functions must set `FERROSA_TEST_CONTAINERS=1` and
//! panic with a diagnostic message if it is absent.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tracing::{debug, info};

use crate::config::Topology;

// CQL ports assigned to each of the three Jepsen cluster nodes on localhost.
const NODE_CQL_PORTS: [u16; 3] = [19042, 19043, 19044];

/// Detect whether `docker` or `podman` is available and return the binary name.
///
/// Checks `docker` first (most common on Linux CI), then falls back to `podman`.
/// Returns `"docker"` or `"podman"`. Panics if neither is found in PATH.
pub fn container_runtime() -> &'static str {
    // Check docker first, then podman.
    for candidate in &["docker", "podman"] {
        let found = std::process::Command::new(candidate)
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if found {
            // Return a &'static str by matching on the known candidates.
            if *candidate == "docker" {
                return "docker";
            } else {
                return "podman";
            }
        }
    }
    panic!("neither 'docker' nor 'podman' found in PATH — install one to run container tests");
}

/// Information about a single provisioned node.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Hostname used to reach this node from the test runner.
    pub host: &'static str,
    /// CQL port on the test runner host.
    pub cql_port: u16,
}

impl NodeInfo {
    /// CQL contact address as `host:port`.
    pub fn cql_address(&self) -> String {
        format!("{}:{}", self.host, self.cql_port)
    }

    /// Return `true` if the CQL port accepts a TCP connection.
    pub async fn cql_reachable(&self) -> bool {
        tokio::net::TcpStream::connect(self.cql_address())
            .await
            .is_ok()
    }
}

/// Information about a provisioned Docker cluster.
#[derive(Debug)]
pub struct ClusterInfo {
    /// The nodes in this cluster, in order.
    pub nodes: Vec<NodeInfo>,
    /// Compose file used to provision the cluster.
    compose_file: PathBuf,
}

impl ClusterInfo {
    /// Assert that the number of nodes matches the topology.
    pub fn assert_node_count(&self, topology: Topology) {
        assert_eq!(
            self.nodes.len(),
            topology.node_count(),
            "expected {} nodes for {:?}, got {}",
            topology.node_count(),
            topology,
            self.nodes.len()
        );
    }
}

/// Locate the compose file relative to the Cargo workspace root.
///
/// Uses `CARGO_MANIFEST_DIR` when available (set during `cargo test`), else
/// walks upward from the current directory looking for `Cargo.toml`.
fn compose_file_path() -> Result<PathBuf> {
    // During `cargo test -p ferrosa-jepsen`, CARGO_MANIFEST_DIR is the
    // ferrosa-jepsen crate directory.
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."));

    let path = manifest_dir.join("tests/docker/jepsen-cluster.yml");
    if path.exists() {
        return Ok(path);
    }

    // Fallback: walk up to find the workspace root containing the compose file.
    let mut dir = manifest_dir.canonicalize().unwrap_or(manifest_dir);
    for _ in 0..5 {
        let candidate = dir.join("ferrosa-jepsen/tests/docker/jepsen-cluster.yml");
        if candidate.exists() {
            return Ok(candidate);
        }
        match dir.parent() {
            Some(p) => dir = p.to_path_buf(),
            None => break,
        }
    }

    bail!(
        "jepsen-cluster.yml not found; expected at ferrosa-jepsen/tests/docker/jepsen-cluster.yml"
    )
}

/// Provision a Docker cluster for the given topology.
///
/// Only `Topology::T1` (3-node) is supported via Docker; larger topologies
/// require Firecracker or Fly.io.
///
/// Callers must ensure `FERROSA_TEST_CONTAINERS=1` is set before calling.
pub async fn provision_docker_cluster(topology: Topology) -> Result<ClusterInfo> {
    if topology.node_count() > NODE_CQL_PORTS.len() {
        bail!(
            "Docker provisioning only supports up to {}-node clusters; requested {:?} ({} nodes)",
            NODE_CQL_PORTS.len(),
            topology,
            topology.node_count()
        );
    }

    let compose_file = compose_file_path()?;
    let rt = container_runtime();

    info!(
        ?topology,
        compose_file = %compose_file.display(),
        runtime = rt,
        "provisioning Docker cluster"
    );

    // Tear down any previous stack to start from a clean state.
    let _ = std::process::Command::new(rt)
        .args([
            "compose",
            "-f",
            compose_file.to_str().unwrap(),
            "down",
            "--remove-orphans",
        ])
        .status();

    // Start the compose stack in detached mode.
    let status = std::process::Command::new(rt)
        .args([
            "compose",
            "-f",
            compose_file.to_str().unwrap(),
            "up",
            "-d",
            "--build",
            "--remove-orphans",
        ])
        .status()
        .context("failed to run compose up")?;

    if !status.success() {
        bail!("compose up failed with exit status {}", status);
    }

    let node_count = topology.node_count();
    let nodes: Vec<NodeInfo> = NODE_CQL_PORTS[..node_count]
        .iter()
        .map(|&port| NodeInfo {
            host: "localhost",
            cql_port: port,
        })
        .collect();

    let cluster = ClusterInfo {
        nodes,
        compose_file,
    };

    // Wait for all nodes to accept CQL connections.
    wait_for_cluster_ready(&cluster).await?;

    info!(node_count, "Docker cluster ready");
    Ok(cluster)
}

/// Wait for all nodes in the cluster to accept TCP connections on their CQL port.
///
/// Polls every 500 ms for up to `CLUSTER_READY_TIMEOUT`.
async fn wait_for_cluster_ready(cluster: &ClusterInfo) -> Result<()> {
    const CLUSTER_READY_TIMEOUT: Duration = Duration::from_secs(120);
    const POLL_INTERVAL: Duration = Duration::from_millis(500);

    let deadline = tokio::time::Instant::now() + CLUSTER_READY_TIMEOUT;

    for node in &cluster.nodes {
        debug!(
            cql_address = node.cql_address(),
            "waiting for CQL readiness"
        );

        loop {
            if node.cql_reachable().await {
                info!(cql_address = node.cql_address(), "CQL port ready");
                break;
            }
            if tokio::time::Instant::now() > deadline {
                bail!(
                    "node {} did not become CQL-ready within {:?}",
                    node.cql_address(),
                    CLUSTER_READY_TIMEOUT
                );
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    Ok(())
}

/// Tear down the Docker cluster started by `provision_docker_cluster`.
///
/// Runs `compose down -v` to remove containers and volumes.
pub async fn teardown_docker_cluster(cluster: ClusterInfo) -> Result<()> {
    let rt = container_runtime();
    let compose_file = cluster.compose_file.to_str().unwrap().to_owned();

    info!(compose_file = %compose_file, runtime = rt, "tearing down Docker cluster");

    let status = std::process::Command::new(rt)
        .args(["compose", "-f", &compose_file, "down", "-v"])
        .status()
        .context("failed to run compose down")?;

    if !status.success() {
        bail!("compose down failed with exit status {}", status);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_info_cql_address() {
        let node = NodeInfo {
            host: "localhost",
            cql_port: 19042,
        };
        assert_eq!(node.cql_address(), "localhost:19042");
    }

    #[test]
    fn cluster_info_assert_node_count_passes() {
        let cluster = ClusterInfo {
            nodes: vec![
                NodeInfo {
                    host: "localhost",
                    cql_port: 19042,
                },
                NodeInfo {
                    host: "localhost",
                    cql_port: 19043,
                },
                NodeInfo {
                    host: "localhost",
                    cql_port: 19044,
                },
            ],
            compose_file: PathBuf::from("/tmp/fake.yml"),
        };
        // T1 = 3 nodes — should not panic.
        cluster.assert_node_count(Topology::T1);
    }

    #[test]
    fn container_runtime_returns_known_value() {
        let rt = container_runtime();
        assert!(
            rt == "docker" || rt == "podman",
            "container_runtime() returned unexpected value: {rt}"
        );
    }

    /// Provision a 3-node Docker cluster and verify all CQL ports are reachable.
    /// Requires: FERROSA_TEST_CONTAINERS=1
    #[tokio::test]
    async fn orchestrator_docker_cluster_provision() {
        if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
            panic!(
                "FERROSA_TEST_CONTAINERS not set — \
                 set FERROSA_TEST_CONTAINERS=1 and ensure docker/podman is available"
            );
        }

        let cluster = provision_docker_cluster(Topology::T1).await.unwrap();

        assert_eq!(cluster.nodes.len(), 3, "T1 topology must yield 3 nodes");

        for node in &cluster.nodes {
            assert!(
                node.cql_reachable().await,
                "node {} should accept CQL connections after provisioning",
                node.cql_address()
            );
        }

        teardown_docker_cluster(cluster).await.unwrap();
    }

    /// Provision, then tear down, and verify containers are removed.
    /// Requires: FERROSA_TEST_CONTAINERS=1
    #[tokio::test]
    async fn orchestrator_cluster_teardown() {
        if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
            panic!(
                "FERROSA_TEST_CONTAINERS not set — \
                 set FERROSA_TEST_CONTAINERS=1 and ensure docker/podman is available"
            );
        }

        let cluster = provision_docker_cluster(Topology::T1).await.unwrap();
        teardown_docker_cluster(cluster).await.unwrap();

        // Verify containers are gone by checking `compose ps` output.
        let compose_file = compose_file_path().expect("compose file should exist");
        let rt = container_runtime();
        let output = std::process::Command::new(rt)
            .args([
                "ps",
                "-a",
                "--filter",
                "name=ferrosa-jepsen",
                "--format",
                "{{.Names}}",
            ])
            .output()
            .expect("docker/podman ps should succeed");

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            !stdout.contains("ferrosa-jepsen-node"),
            "ferrosa-jepsen node containers should be removed after teardown, found: {stdout}"
        );

        // Unused in the assertion above but kept to avoid dead-code warning.
        let _ = compose_file;
    }
}
