use std::path::PathBuf;

/// Runtime cluster environment for integration tests.
///
/// Callers should call [`TestClusterEnv::detect`] at the start of each test.
/// When it returns `None`, the test should print a skip message and return
/// early — the test is treated as passing (not failed, not ignored).
pub struct TestClusterEnv {
    /// Comma-separated "host:cql_port" pairs, e.g. "127.0.0.1:9042,127.0.0.1:9043"
    pub cql_nodes: Vec<String>,
    /// SSH key for connecting to the VMs.
    pub ssh_key: PathBuf,
    /// SSH port (Lima forwards to this port on localhost).
    pub ssh_port: u16,
    /// SSH host.
    pub ssh_host: String,
    /// Whether to provision new Firecracker VMs (true) or use a pre-existing cluster (false).
    pub firecracker_provision: bool,
}

impl TestClusterEnv {
    /// Detect cluster configuration from environment variables.
    ///
    /// Checks (in order):
    /// 1. `FERROSA_TEST_CLUSTER_NODES` — comma-separated host:port pairs for a pre-existing cluster
    /// 2. `FERROSA_TEST_FIRECRACKER=1` — provision a new Firecracker cluster via Lima
    ///
    /// Returns `None` if neither is set — callers should skip the test.
    pub fn detect() -> Option<Self> {
        let ssh_key = std::env::var("FERROSA_TEST_VM_KEY")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("rootfs/test_key"));

        let ssh_port: u16 = std::env::var("FERROSA_TEST_VM_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2022);

        let ssh_host = std::env::var("FERROSA_TEST_VM_HOST")
            .unwrap_or_else(|_| "127.0.0.1".to_string());

        if let Ok(nodes) = std::env::var("FERROSA_TEST_CLUSTER_NODES") {
            let cql_nodes = nodes.split(',').map(str::to_string).collect();
            return Some(Self {
                cql_nodes,
                ssh_key,
                ssh_port,
                ssh_host,
                firecracker_provision: false,
            });
        }

        if std::env::var("FERROSA_TEST_FIRECRACKER").is_ok() {
            // Single node for now; expand to N nodes when multi-VM setup is ready.
            let node_addr = format!("{ssh_host}:9042");
            return Some(Self {
                cql_nodes: vec![node_addr],
                ssh_key,
                ssh_port,
                ssh_host,
                firecracker_provision: true,
            });
        }

        None
    }

    /// Whether any cluster environment is available.
    pub fn available() -> bool {
        Self::detect().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Serialize tests that mutate environment variables — Rust runs tests in
    // parallel threads by default, and detect() reads CLUSTER_NODES before
    // FIRECRACKER, so concurrent mutation causes a false firecracker_provision=false.
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn detect_returns_none_without_env() {
        let _g = ENV_MUTEX.lock().unwrap();
        if std::env::var("FERROSA_TEST_CLUSTER_NODES").is_err()
            && std::env::var("FERROSA_TEST_FIRECRACKER").is_err()
        {
            assert!(TestClusterEnv::detect().is_none());
            assert!(!TestClusterEnv::available());
        }
    }

    #[test]
    fn detect_cluster_nodes_from_env() {
        let _g = ENV_MUTEX.lock().unwrap();
        unsafe {
            std::env::set_var("FERROSA_TEST_CLUSTER_NODES", "10.0.0.1:9042,10.0.0.2:9042");
        }
        let env = TestClusterEnv::detect();
        unsafe {
            std::env::remove_var("FERROSA_TEST_CLUSTER_NODES");
        }
        let env = env.expect("should detect cluster from FERROSA_TEST_CLUSTER_NODES");
        assert_eq!(env.cql_nodes.len(), 2);
        assert_eq!(env.cql_nodes[0], "10.0.0.1:9042");
        assert!(!env.firecracker_provision);
    }

    #[test]
    fn detect_firecracker_from_env() {
        let _g = ENV_MUTEX.lock().unwrap();
        // Also clear FERROSA_TEST_CLUSTER_NODES in case another test set it before
        // we acquired the lock (e.g. if the lock wasn't held during the set).
        let saved_cluster = std::env::var("FERROSA_TEST_CLUSTER_NODES").ok();
        unsafe {
            std::env::remove_var("FERROSA_TEST_CLUSTER_NODES");
            std::env::set_var("FERROSA_TEST_FIRECRACKER", "1");
        }
        let env = TestClusterEnv::detect();
        unsafe {
            std::env::remove_var("FERROSA_TEST_FIRECRACKER");
            if let Some(v) = saved_cluster {
                std::env::set_var("FERROSA_TEST_CLUSTER_NODES", v);
            }
        }
        let env = env.expect("should detect Firecracker from FERROSA_TEST_FIRECRACKER");
        assert!(env.firecracker_provision);
        assert_eq!(env.cql_nodes.len(), 1);
    }
}
