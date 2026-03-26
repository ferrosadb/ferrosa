use std::path::{Path, PathBuf};

use anyhow::Result;
use async_trait::async_trait;

use super::{DriverConfig, DriverRunner};
use crate::history::{History, HistoryRecorder, Op, OpResult};

/// In-process Rust driver (no container needed).
pub struct RustDriver;

#[async_trait]
impl DriverRunner for RustDriver {
    fn name(&self) -> &str {
        "rust"
    }

    async fn run(&self, config: &DriverConfig) -> Result<PathBuf> {
        let output_path = config
            .output_dir
            .join(format!("{}.jsonl", config.client_id));

        // In-process execution using HistoryRecorder
        let mut recorder = HistoryRecorder::new(&config.client_id);

        // Placeholder: record a single write
        // TODO: Wire to actual CQL session + workload
        recorder.invoke(Op::Write {
            key: "test".into(),
            value: 1,
        });
        recorder.complete(OpResult::Ok);

        let history = recorder.finish();
        history.to_jsonl(&output_path)?;

        Ok(output_path)
    }

    async fn collect_history(&self, output_path: &Path) -> Result<History> {
        History::from_jsonl(output_path)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::driver::DriverKind;

    #[tokio::test]
    async fn rust_driver_produces_history() {
        let dir = tempfile::tempdir().unwrap();
        let config = DriverConfig {
            kind: DriverKind::Rust,
            contact_points: vec!["127.0.0.1:9042".into()],
            workload: "register".into(),
            duration: Duration::from_secs(1),
            threads: 1,
            output_dir: dir.path().to_path_buf(),
            client_id: "test-rust".into(),
        };
        let driver = RustDriver;
        let path = driver.run(&config).await.unwrap();
        assert!(path.exists());
        let history = driver.collect_history(&path).await.unwrap();
        assert!(!history.is_empty());
    }
}
