use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::history::History;

pub mod rust_driver;

/// Supported driver languages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
pub enum DriverKind {
    Rust,
    Python,
    Go,
    Node,
    Java,
    CSharp,
}

impl DriverKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::Python => "python",
            Self::Go => "go",
            Self::Node => "node",
            Self::Java => "java",
            Self::CSharp => "csharp",
        }
    }

    /// Docker image name for this driver.
    pub fn image(self) -> &'static str {
        match self {
            Self::Rust => "ferrosa-jepsen-rust",
            Self::Python => "ferrosa-jepsen-python",
            Self::Go => "ferrosa-jepsen-go",
            Self::Node => "ferrosa-jepsen-node",
            Self::Java => "ferrosa-jepsen-java",
            Self::CSharp => "ferrosa-jepsen-csharp",
        }
    }
}

/// Configuration for a driver run.
#[derive(Debug, Clone)]
pub struct DriverConfig {
    pub kind: DriverKind,
    pub contact_points: Vec<String>,
    pub workload: String,
    pub duration: Duration,
    pub threads: usize,
    pub output_dir: PathBuf,
    pub client_id: String,
}

/// A driver that can run workloads against a cluster.
#[async_trait]
pub trait DriverRunner: Send + Sync {
    /// Name of this driver.
    fn name(&self) -> &str;

    /// Run the workload, producing a history file.
    async fn run(&self, config: &DriverConfig) -> Result<PathBuf>;

    /// Collect the history from the output file.
    async fn collect_history(&self, output_path: &Path) -> Result<History>;
}

/// Registry of available drivers.
pub struct DriverRegistry {
    drivers: Vec<Box<dyn DriverRunner>>,
}

impl DriverRegistry {
    pub fn new() -> Self {
        Self {
            drivers: Vec::new(),
        }
    }

    pub fn register(&mut self, driver: Box<dyn DriverRunner>) {
        self.drivers.push(driver);
    }

    pub fn get(&self, name: &str) -> Option<&dyn DriverRunner> {
        self.drivers
            .iter()
            .find(|d| d.name() == name)
            .map(|b| b.as_ref())
    }

    pub fn names(&self) -> Vec<String> {
        self.drivers.iter().map(|d| d.name().to_string()).collect()
    }

    /// Phase 1: Rust driver only.
    pub fn phase1() -> Self {
        let mut reg = Self::new();
        reg.register(Box::new(rust_driver::RustDriver));
        reg
    }

    /// Phase 2: All 6 drivers.
    pub fn phase2() -> Self {
        let mut reg = Self::phase1();
        reg.register(Box::new(ContainerDriver::new(DriverKind::Python)));
        reg.register(Box::new(ContainerDriver::new(DriverKind::Go)));
        reg.register(Box::new(ContainerDriver::new(DriverKind::Node)));
        reg.register(Box::new(ContainerDriver::new(DriverKind::Java)));
        reg.register(Box::new(ContainerDriver::new(DriverKind::CSharp)));
        reg
    }
}

/// Generic container-based driver (Python, Go, Node, Java, C#).
/// Spawns a Docker container that runs the workload and writes JSONL.
pub struct ContainerDriver {
    kind: DriverKind,
}

impl ContainerDriver {
    pub fn new(kind: DriverKind) -> Self {
        Self { kind }
    }
}

#[async_trait]
impl DriverRunner for ContainerDriver {
    fn name(&self) -> &str {
        self.kind.name()
    }

    async fn run(&self, config: &DriverConfig) -> Result<PathBuf> {
        let output_path = config
            .output_dir
            .join(format!("{}.jsonl", config.client_id));

        // Build docker run command
        let contact_points = config.contact_points.join(",");
        let cmd = format!(
            "docker run --rm --network host \
             -v {}:/output \
             {} \
             --contact-points {} \
             --workload {} \
             --duration {} \
             --threads {} \
             --output-dir /output \
             --client-id {}",
            config.output_dir.display(),
            self.kind.image(),
            contact_points,
            config.workload,
            config.duration.as_secs(),
            config.threads,
            config.client_id,
        );

        tracing::info!(driver = self.kind.name(), cmd = %cmd, "Starting container driver");

        let output = tokio::process::Command::new("sh")
            .args(["-c", &cmd])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Driver {} failed: {}", self.kind.name(), stderr);
        }

        Ok(output_path)
    }

    async fn collect_history(&self, output_path: &Path) -> Result<History> {
        History::from_jsonl(output_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn driver_registry_phase1() {
        let reg = DriverRegistry::phase1();
        assert_eq!(reg.names(), vec!["rust"]);
    }

    #[test]
    fn driver_registry_phase2() {
        let reg = DriverRegistry::phase2();
        assert_eq!(reg.names().len(), 6);
        assert!(reg.get("rust").is_some());
        assert!(reg.get("python").is_some());
        assert!(reg.get("go").is_some());
    }

    #[test]
    fn driver_kind_names() {
        assert_eq!(DriverKind::Rust.name(), "rust");
        assert_eq!(DriverKind::Python.name(), "python");
        assert_eq!(DriverKind::CSharp.name(), "csharp");
    }
}
