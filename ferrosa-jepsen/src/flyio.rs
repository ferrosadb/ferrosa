use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Fly.io machine configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlyMachineConfig {
    pub app_name: String,
    pub region: String,
    pub size: String, // "shared-cpu-1x", "performance-1x", etc.
    pub image: String,
    pub env: std::collections::HashMap<String, String>,
}

/// A provisioned Fly.io machine.
#[derive(Debug, Clone)]
pub struct FlyMachine {
    pub id: String,
    pub name: String,
    pub region: String,
    pub private_ip: String,
    pub state: MachineState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MachineState {
    Created,
    Starting,
    Started,
    Stopping,
    Stopped,
    Destroying,
    Destroyed,
}

/// Fly.io machine manager using flyctl/Machines API.
pub struct FlyIoProvisioner {
    api_token: String,
    org: String,
}

impl FlyIoProvisioner {
    pub fn new(api_token: String, org: String) -> Self {
        Self { api_token, org }
    }

    pub fn from_env() -> Result<Self> {
        let token = std::env::var("FLYCTL_API_TOKEN").context("FLYCTL_API_TOKEN not set")?;
        let org = std::env::var("FLY_ORG").unwrap_or_else(|_| "personal".into());
        Ok(Self::new(token, org))
    }

    /// Create a Fly.io machine.
    pub async fn create_machine(&self, config: &FlyMachineConfig) -> Result<FlyMachine> {
        let output = tokio::process::Command::new("flyctl")
            .args([
                "machines",
                "run",
                &config.image,
                "--app",
                &config.app_name,
                "--region",
                &config.region,
                "--size",
                &config.size,
                "--json",
            ])
            .env("FLY_API_TOKEN", &self.api_token)
            .output()
            .await
            .context("Failed to run flyctl")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("flyctl create failed: {}", stderr);
        }

        let json: serde_json::Value =
            serde_json::from_slice(&output.stdout).context("Failed to parse flyctl output")?;

        Ok(FlyMachine {
            id: json["id"].as_str().unwrap_or("").into(),
            name: json["name"].as_str().unwrap_or("").into(),
            region: config.region.clone(),
            private_ip: json["private_ip"].as_str().unwrap_or("").into(),
            state: MachineState::Started,
        })
    }

    /// Wait for a machine to reach Started state.
    pub async fn wait_started(&self, app: &str, machine_id: &str, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > timeout {
                anyhow::bail!("Timeout waiting for machine {} to start", machine_id);
            }

            let output = tokio::process::Command::new("flyctl")
                .args(["machines", "status", machine_id, "--app", app, "--json"])
                .env("FLY_API_TOKEN", &self.api_token)
                .output()
                .await?;

            if output.status.success() {
                let json: serde_json::Value = serde_json::from_slice(&output.stdout)?;
                if json["state"].as_str() == Some("started") {
                    return Ok(());
                }
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    /// Destroy a machine.
    pub async fn destroy_machine(&self, app: &str, machine_id: &str) -> Result<()> {
        let output = tokio::process::Command::new("flyctl")
            .args(["machines", "destroy", machine_id, "--app", app, "--force"])
            .env("FLY_API_TOKEN", &self.api_token)
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::warn!("Failed to destroy machine {}: {}", machine_id, stderr);
        }
        Ok(())
    }

    /// List all machines for an app.
    pub async fn list_machines(&self, app: &str) -> Result<Vec<FlyMachine>> {
        let output = tokio::process::Command::new("flyctl")
            .args(["machines", "list", "--app", app, "--json"])
            .env("FLY_API_TOKEN", &self.api_token)
            .output()
            .await?;

        if !output.status.success() {
            return Ok(vec![]);
        }

        let machines: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout)?;
        Ok(machines
            .iter()
            .map(|m| FlyMachine {
                id: m["id"].as_str().unwrap_or("").into(),
                name: m["name"].as_str().unwrap_or("").into(),
                region: m["region"].as_str().unwrap_or("").into(),
                private_ip: m["private_ip"].as_str().unwrap_or("").into(),
                state: match m["state"].as_str() {
                    Some("started") => MachineState::Started,
                    Some("stopped") => MachineState::Stopped,
                    _ => MachineState::Created,
                },
            })
            .collect())
    }

    /// Create a Fly.io app if it doesn't exist.
    pub async fn ensure_app(&self, app_name: &str) -> Result<()> {
        let output = tokio::process::Command::new("flyctl")
            .args(["apps", "create", app_name, "--org", &self.org, "--json"])
            .env("FLY_API_TOKEN", &self.api_token)
            .output()
            .await?;

        // Ignore "already exists" errors
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.contains("already exists") {
                anyhow::bail!("Failed to create app: {}", stderr);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fly_machine_config_serialization() {
        let config = FlyMachineConfig {
            app_name: "ferrosa-jepsen-test".into(),
            region: "iad".into(),
            size: "shared-cpu-1x".into(),
            image: "ferrosa:latest".into(),
            env: std::collections::HashMap::new(),
        };
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("ferrosa-jepsen-test"));
    }

    #[test]
    fn machine_state_values() {
        assert_eq!(MachineState::Started, MachineState::Started);
        assert_ne!(MachineState::Started, MachineState::Stopped);
    }
}
