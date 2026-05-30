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
const NODE_CQL_PORTS: [u16; 3] = [49042, 49043, 49044];

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
    pub(crate) compose_file: PathBuf,
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

/// Parse a docker-compose file and extract each `nodeN` service's
/// `FERROSA_SEED` environment variable (or `None` if unset).
///
/// Used by Sprint 2 W2.14 to assert seed-list symmetry: every node must
/// list every other node in its seed env so cluster bring-up does not
/// depend on a single bootstrap host.
///
/// Implementation is a simple line-oriented parser because we don't need
/// a full YAML dependency for this one assertion.
pub(crate) fn parse_seeds_from_compose(
    yaml: &str,
) -> std::collections::BTreeMap<String, Vec<String>> {
    use std::collections::BTreeMap;
    let mut result: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut current_node: Option<String> = None;
    let mut in_environment = false;

    for raw in yaml.lines() {
        let trimmed = raw.trim();
        // Skip comments and blanks.
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        // Service-level header: `node1:` etc., indented exactly two spaces.
        // (services: header lives at column 0; service entries at column 2.)
        let leading_spaces = raw.len() - raw.trim_start().len();
        if leading_spaces == 2 && trimmed.ends_with(':') {
            let name = trimmed.trim_end_matches(':').to_string();
            if name.starts_with("node") {
                current_node = Some(name);
                result.entry(current_node.clone().unwrap()).or_default();
                in_environment = false;
            } else {
                current_node = None;
                in_environment = false;
            }
            continue;
        }

        // Environment block opener.
        if current_node.is_some() && trimmed == "environment:" {
            in_environment = true;
            continue;
        }

        // Reset on a sibling at column 4 that isn't an env entry.
        if leading_spaces == 4 && trimmed.ends_with(':') && in_environment {
            in_environment = false;
        }

        if in_environment && trimmed.starts_with("FERROSA_SEED:") {
            if let Some(node) = &current_node {
                let value = trimmed
                    .trim_start_matches("FERROSA_SEED:")
                    .trim()
                    .trim_matches('"')
                    .to_string();
                let seeds: Vec<String> = value
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                result.insert(node.clone(), seeds);
            }
        }
    }

    result
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
            cql_port: 49042,
        };
        assert_eq!(node.cql_address(), "localhost:49042");
    }

    #[test]
    fn cluster_info_assert_node_count_passes() {
        let cluster = ClusterInfo {
            nodes: vec![
                NodeInfo {
                    host: "localhost",
                    cql_port: 49042,
                },
                NodeInfo {
                    host: "localhost",
                    cql_port: 49043,
                },
                NodeInfo {
                    host: "localhost",
                    cql_port: 49044,
                },
            ],
            compose_file: PathBuf::from("/tmp/fake.yml"),
        };
        // T1 = 3 nodes — should not panic.
        cluster.assert_node_count(Topology::T1);
    }

    /// Sanity check the FERROSA_SEED parser on a hand-crafted snippet.
    #[test]
    fn parse_seeds_from_compose_extracts_per_node_lists() {
        let yaml = r#"
services:
  rustfs:
    image: rustfs
  node1:
    environment:
      FERROSA_HOST_ID: "11111111-1111-1111-1111-111111111111"
      FERROSA_SEED: "node2:7000,node3:7000"
  node2:
    environment:
      FERROSA_SEED: "node1:7000,node3:7000"
  node3:
    environment:
      FERROSA_SEED: "node1:7000,node2:7000"
"#;
        let map = parse_seeds_from_compose(yaml);
        assert_eq!(
            map.get("node1").map(|v| v.as_slice()),
            Some(["node2:7000".to_string(), "node3:7000".to_string()].as_slice())
        );
        assert_eq!(
            map.get("node2").map(|v| v.as_slice()),
            Some(["node1:7000".to_string(), "node3:7000".to_string()].as_slice())
        );
        assert_eq!(
            map.get("node3").map(|v| v.as_slice()),
            Some(["node1:7000".to_string(), "node2:7000".to_string()].as_slice())
        );
    }

    /// Parser must record nodes that have no FERROSA_SEED entry as having an
    /// empty seed list (used to detect the asymmetric pre-Sprint-2 config).
    #[test]
    fn parse_seeds_from_compose_records_missing_seed_as_empty() {
        let yaml = r#"
services:
  node1:
    environment:
      FERROSA_HOST_ID: "11111111-1111-1111-1111-111111111111"
  node2:
    environment:
      FERROSA_SEED: "node1:7000"
"#;
        let map = parse_seeds_from_compose(yaml);
        assert_eq!(map.get("node1"), Some(&Vec::<String>::new()));
        assert_eq!(
            map.get("node2").map(|v| v.as_slice()),
            Some(["node1:7000".to_string()].as_slice())
        );
    }

    /// W2.14: every nodeN service in jepsen-cluster.yml must list every other
    /// node in its FERROSA_SEED env. Asymmetric seed config (node1 with no seed,
    /// node2/node3 seeded only by node1) is the pre-Sprint-2 baseline and is
    /// exactly what causes "random startup order" formation flakes.
    #[test]
    fn cluster_yml_seed_list_is_symmetric() {
        let path = compose_file_path()
            .expect("jepsen-cluster.yml must be discoverable from CARGO_MANIFEST_DIR");
        let yaml = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {}", path.display(), e));
        let seeds = parse_seeds_from_compose(&yaml);

        assert!(
            !seeds.is_empty(),
            "expected at least one nodeN service in the compose file"
        );

        let node_names: std::collections::BTreeSet<String> = seeds.keys().cloned().collect();
        assert!(
            node_names.len() >= 3,
            "Jepsen compose must declare at least 3 ferrosa nodes for a meaningful raft cluster; \
             got {node_names:?}"
        );

        for (node, declared_seeds) in &seeds {
            // Build the set of expected seeds: every other node's `nodeX:7000`.
            let expected: std::collections::BTreeSet<String> = node_names
                .iter()
                .filter(|n| n.as_str() != node.as_str())
                .map(|n| format!("{n}:7000"))
                .collect();

            let actual: std::collections::BTreeSet<String> =
                declared_seeds.iter().cloned().collect();

            assert_eq!(
                actual, expected,
                "node {node} must seed every other node (symmetric seed config — Sprint 2 W2.14). \
                 expected {expected:?}, got {actual:?}"
            );
        }
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
    #[cfg(feature = "live-infra-tests")]
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
    #[cfg(feature = "live-infra-tests")]
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
