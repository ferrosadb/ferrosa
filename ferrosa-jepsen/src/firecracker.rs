use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{bail, Context, Result};
use serde_json::json;
use tokio::process::{Child, Command};
use tracing::{debug, info, warn};

/// Configuration for a single Firecracker microVM.
#[derive(Debug, Clone)]
pub struct VmConfig {
    /// Number of virtual CPUs.
    pub vcpu: u32,
    /// Memory in megabytes.
    pub mem_mb: u32,
    /// Path to the root filesystem image.
    pub rootfs: PathBuf,
    /// Path to the uncompressed Linux kernel (vmlinux).
    pub kernel: PathBuf,
    /// TAP device name for networking (e.g., "tap0").
    pub tap_device: String,
    /// Guest IP address (e.g., "172.16.0.2").
    pub ip: String,
    /// Gateway IP address (e.g., "172.16.0.1").
    pub gateway: String,
    /// Path for the Firecracker API Unix socket.
    pub socket_path: PathBuf,
}

impl VmConfig {
    /// Create a test configuration. Only useful with actual firecracker + rootfs.
    #[cfg(test)]
    fn default_test() -> Self {
        Self {
            vcpu: 2,
            mem_mb: 256,
            rootfs: PathBuf::from("rootfs/rootfs.ext4"),
            kernel: PathBuf::from("rootfs/vmlinux"),
            tap_device: "tap0".to_string(),
            ip: "172.16.0.2".to_string(),
            gateway: "172.16.0.1".to_string(),
            socket_path: PathBuf::from("/tmp/ferrosa-jepsen-test.sock"),
        }
    }
}

/// A running Firecracker microVM instance.
///
/// Manages the lifecycle of a single Firecracker process, including
/// creation via the REST API, health checks, and teardown.
pub struct FirecrackerVm {
    config: VmConfig,
    process: Child,
    socket_path: PathBuf,
}

impl FirecrackerVm {
    /// Start a Firecracker VM with the given configuration.
    ///
    /// This launches the firecracker process, then configures it via the
    /// REST API (over the Unix socket) before issuing InstanceStart.
    pub async fn create(config: VmConfig) -> Result<Self> {
        // Clean up any leftover socket from a previous run.
        if config.socket_path.exists() {
            std::fs::remove_file(&config.socket_path)
                .context("removing stale firecracker socket")?;
        }

        let socket_path = config.socket_path.clone();
        let socket_str = socket_path
            .to_str()
            .context("socket path is not valid UTF-8")?;

        info!(socket = %socket_str, "starting firecracker process");

        let process = Command::new("firecracker")
            .arg("--api-sock")
            .arg(socket_str)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("failed to spawn firecracker — is it installed and in PATH?")?;

        // Wait briefly for the socket to appear.
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        while !socket_path.exists() {
            if tokio::time::Instant::now() > deadline {
                bail!(
                    "firecracker API socket did not appear at {} within 5 s",
                    socket_str
                );
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
        debug!("firecracker socket ready");

        let vm = Self {
            config,
            process,
            socket_path,
        };

        vm.configure_boot_source().await?;
        vm.configure_rootfs().await?;
        vm.configure_network().await?;
        vm.configure_machine().await?;
        vm.start_instance().await?;

        info!("firecracker VM started");
        Ok(vm)
    }

    /// Check whether the firecracker process is still alive.
    pub fn is_running(&mut self) -> bool {
        matches!(self.process.try_wait(), Ok(None))
    }

    /// Gracefully stop the VM: send InstanceHalt, then kill the process
    /// and clean up the socket file.
    pub async fn destroy(&mut self) -> Result<()> {
        info!("destroying firecracker VM");

        // Best-effort halt via API — ignore errors (VM may already be dead).
        if let Err(e) = self.send_action("SendCtrlAltDel").await {
            warn!(error = %e, "halt action failed (VM may already be stopped)");
        }

        // Give the process a moment to exit gracefully.
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Force-kill if still running.
        if let Err(e) = self.process.kill().await {
            debug!(error = %e, "kill returned error (process may have already exited)");
        }

        // Clean up the socket.
        if self.socket_path.exists() {
            std::fs::remove_file(&self.socket_path).ok();
        }

        Ok(())
    }

    // ── REST API helpers (via curl --unix-socket) ──────────────────────

    /// PUT a JSON body to the Firecracker API.
    ///
    /// We shell out to `curl` because reqwest does not natively support
    /// Unix domain sockets and adding hyper-util just for this is not
    /// worth the dependency.
    async fn api_put(&self, path: &str, body: &serde_json::Value) -> Result<String> {
        let socket_str = self
            .socket_path
            .to_str()
            .context("socket path not valid UTF-8")?;

        let body_str = serde_json::to_string(body)?;
        let url = format!("http://localhost{path}");

        debug!(url = %url, "firecracker API PUT");

        let output = Command::new("curl")
            .arg("--unix-socket")
            .arg(socket_str)
            .arg("-s")
            .arg("-X")
            .arg("PUT")
            .arg("-H")
            .arg("Content-Type: application/json")
            .arg("-d")
            .arg(&body_str)
            .arg(&url)
            .output()
            .await
            .context("failed to execute curl")?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        if !output.status.success() {
            bail!(
                "curl PUT {url} failed (exit {}): stdout={stdout}, stderr={stderr}",
                output.status
            );
        }

        // Firecracker returns 204 on success (empty body) or an error JSON.
        // If the body contains "fault_message", treat it as an error.
        if stdout.contains("fault_message") {
            bail!("firecracker API error on PUT {url}: {stdout}");
        }

        Ok(stdout)
    }

    async fn configure_boot_source(&self) -> Result<()> {
        let kernel = self
            .config
            .kernel
            .to_str()
            .context("kernel path not valid UTF-8")?;

        let boot_args = format!(
            "console=ttyS0 reboot=k panic=1 pci=off ip={}::{}:255.255.255.0::eth0:off",
            self.config.ip, self.config.gateway
        );

        self.api_put(
            "/boot-source",
            &json!({
                "kernel_image_path": kernel,
                "boot_args": boot_args,
            }),
        )
        .await
        .context("configure boot source")?;

        Ok(())
    }

    async fn configure_rootfs(&self) -> Result<()> {
        let rootfs = self
            .config
            .rootfs
            .to_str()
            .context("rootfs path not valid UTF-8")?;

        self.api_put(
            "/drives/rootfs",
            &json!({
                "drive_id": "rootfs",
                "path_on_host": rootfs,
                "is_root_device": true,
                "is_read_only": false,
            }),
        )
        .await
        .context("configure rootfs drive")?;

        Ok(())
    }

    async fn configure_network(&self) -> Result<()> {
        self.api_put(
            "/network-interfaces/eth0",
            &json!({
                "iface_id": "eth0",
                "host_dev_name": self.config.tap_device,
            }),
        )
        .await
        .context("configure network interface")?;

        Ok(())
    }

    async fn configure_machine(&self) -> Result<()> {
        self.api_put(
            "/machine-config",
            &json!({
                "vcpu_count": self.config.vcpu,
                "mem_size_mib": self.config.mem_mb,
            }),
        )
        .await
        .context("configure machine")?;

        Ok(())
    }

    async fn start_instance(&self) -> Result<()> {
        self.api_put(
            "/actions",
            &json!({
                "action_type": "InstanceStart",
            }),
        )
        .await
        .context("start instance")?;

        Ok(())
    }

    async fn send_action(&self, action: &str) -> Result<()> {
        self.api_put(
            "/actions",
            &json!({
                "action_type": action,
            }),
        )
        .await
        .context("send action")?;

        Ok(())
    }
}

impl Drop for FirecrackerVm {
    fn drop(&mut self) {
        // Best-effort cleanup: remove the socket file.
        // We cannot run async code in Drop, so we just clean up the file
        // and rely on kill_on_drop on the child process.
        if self.socket_path.exists() {
            std::fs::remove_file(&self.socket_path).ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vm_config_default_test_values() {
        let cfg = VmConfig::default_test();
        assert_eq!(cfg.vcpu, 2);
        assert_eq!(cfg.mem_mb, 256);
        assert_eq!(cfg.tap_device, "tap0");
    }

    /// Requires firecracker binary, root privileges, and a built rootfs.
    #[tokio::test]
    #[ignore]
    async fn provision_single_vm() {
        let mut vm = FirecrackerVm::create(VmConfig::default_test())
            .await
            .unwrap();
        assert!(vm.is_running());
        vm.destroy().await.unwrap();
    }
}
