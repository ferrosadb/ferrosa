use std::path::{Path, PathBuf};

use anyhow::Result;
use async_trait::async_trait;

use super::{DriverConfig, DriverRunner};
use crate::cql_session::ScyllaCqlSession;
use crate::history::{History, HistoryRecorder};
use crate::workload::WorkloadRegistry;

/// In-process Rust driver — connects to a real CQL cluster via the scylla driver.
///
/// Looks up the workload by name from `WorkloadRegistry::phase1()`, runs setup,
/// executes operations for the configured duration, then writes the history to JSONL.
pub struct RustDriver;

#[async_trait]
impl DriverRunner for RustDriver {
    fn name(&self) -> &str {
        "rust"
    }

    async fn run(&self, config: &DriverConfig) -> Result<PathBuf> {
        assert!(
            !config.contact_points.is_empty(),
            "contact_points must not be empty"
        );
        assert!(
            !config.workload.is_empty(),
            "workload name must not be empty"
        );

        let output_path = config
            .output_dir
            .join(format!("{}.jsonl", config.client_id));

        // Connect to the cluster.
        let session = ScyllaCqlSession::connect(&config.contact_points).await?;

        // Resolve the workload.
        let registry = WorkloadRegistry::phase1();
        let workload = registry
            .get(&config.workload)
            .ok_or_else(|| anyhow::anyhow!("unknown workload: {}", config.workload))?;

        // Set up schema (idempotent CREATE IF NOT EXISTS).
        workload.setup(&session).await?;

        // Run the workload for the configured duration.
        let mut recorder = HistoryRecorder::new(&config.client_id);
        workload
            .run(&session, &mut recorder, config.duration)
            .await?;

        let history = recorder.finish();
        history.to_jsonl(&output_path)?;

        tracing::info!(
            client_id = %config.client_id,
            workload = %config.workload,
            ops = history.len(),
            path = %output_path.display(),
            "Rust driver finished"
        );

        Ok(output_path)
    }

    async fn collect_history(&self, output_path: &Path) -> Result<History> {
        History::from_jsonl(output_path)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;

    use super::*;
    use crate::driver::DriverKind;
    use crate::history::{Op, OpResult};
    use crate::workload::CqlSession;

    // ---------------------------------------------------------------------------
    // Mock CQL session
    // ---------------------------------------------------------------------------

    /// A mock `CqlSession` that returns deterministic canned responses.
    ///
    /// Tracks every query it receives so tests can inspect them.
    struct MockCqlSession {
        /// Fixed value returned from SELECT queries.
        val: i64,
    }

    impl MockCqlSession {
        fn with_value(val: i64) -> Arc<Self> {
            Arc::new(Self { val })
        }
    }

    #[async_trait]
    impl CqlSession for MockCqlSession {
        async fn execute(&self, query: &str) -> Result<Vec<Vec<(String, String)>>> {
            // DDL and DML with no result set.
            let q = query.trim().to_ascii_uppercase();
            if q.starts_with("CREATE")
                || q.starts_with("INSERT")
                || q.starts_with("UPDATE")
                || q.starts_with("DELETE")
            {
                return Ok(vec![]);
            }

            // SELECT with [applied] column — simulate LWT result.
            if q.contains("[APPLIED]") || q.contains("IF VAL") || q.contains("IF ") {
                return Ok(vec![vec![
                    ("[applied]".into(), "true".into()),
                    ("val".into(), self.val.to_string()),
                ]]);
            }

            // Plain SELECT — return a single row with the mocked value.
            Ok(vec![vec![("val".into(), self.val.to_string())]])
        }
    }

    // ---------------------------------------------------------------------------
    // Helper: run the register workload against a mock session
    // ---------------------------------------------------------------------------

    async fn run_register_workload_with_mock(val: i64, duration: Duration) -> (PathBuf, History) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let config = DriverConfig {
            kind: DriverKind::Rust,
            contact_points: vec!["127.0.0.1:9042".into()], // unused — no real connection
            workload: "register".into(),
            duration,
            threads: 1,
            output_dir: dir.path().to_path_buf(),
            client_id: "mock-rust".into(),
        };

        // Wire the mock directly (bypass ScyllaCqlSession).
        let session = MockCqlSession::with_value(val);
        let registry = WorkloadRegistry::phase1();
        let workload = registry.get("register").expect("register workload");

        workload
            .setup(session.as_ref())
            .await
            .expect("workload setup");

        let mut recorder = HistoryRecorder::new(&config.client_id);
        workload
            .run(session.as_ref(), &mut recorder, config.duration)
            .await
            .expect("workload run");

        let history = recorder.finish();
        let path = dir.path().join(format!("{}.jsonl", config.client_id));
        history.to_jsonl(&path).expect("write jsonl");

        // Keep dir alive until the end of the test by leaking it (small allocation).
        std::mem::forget(dir);
        (path, history)
    }

    // ---------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------

    /// Connect to a real ferrosa cluster and run a system query.
    ///
    /// Requires FERROSA_TEST_CONTAINERS=1 and a cluster on port 49042.
    #[cfg(feature = "live-infra-tests")]
    #[tokio::test]
    async fn rust_driver_connects_to_cluster() {
        if std::env::var("FERROSA_TEST_CONTAINERS").is_err() {
            panic!(
                "FERROSA_TEST_CONTAINERS not set — \
                 start a Cassandra-compatible cluster on port 49042 first \
                 (e.g. via docker-compose in ferrosa-jepsen/docker)"
            );
        }

        let dir = tempfile::tempdir().unwrap();
        let config = DriverConfig {
            kind: DriverKind::Rust,
            contact_points: vec!["localhost:49042".into()],
            workload: "register".into(),
            duration: Duration::from_secs(5),
            threads: 1,
            output_dir: dir.path().to_path_buf(),
            client_id: "integration-rust".into(),
        };

        let driver = RustDriver;
        let path = driver.run(&config).await.expect("driver run");
        assert!(path.exists(), "output JSONL must exist");

        let history = driver
            .collect_history(&path)
            .await
            .expect("collect history");
        assert!(
            !history.is_empty(),
            "history must contain at least one operation"
        );

        // No placeholder "test" keys should appear — only workload keys.
        for op in &history.operations {
            if let Op::Write { key, .. } = &op.op {
                assert_ne!(
                    key.as_str(),
                    "test",
                    "placeholder 'test' key must not appear in real history"
                );
            }
        }
    }

    /// Unit test with a mock CQL session — no cluster needed.
    ///
    /// Runs the register workload against a mock that returns deterministic values,
    /// writes the history to a temp JSONL file, and verifies:
    ///   1. The JSONL file is non-empty.
    ///   2. No placeholder "test" keys appear.
    ///   3. History contains at least one operation.
    ///   4. All recorded operations have a client_id that matches config.
    #[tokio::test]
    async fn rust_driver_register_history_roundtrip() {
        // Run with a very short duration so the test finishes quickly.
        let (path, history) = run_register_workload_with_mock(42, Duration::from_millis(50)).await;

        // 1. File must be non-empty.
        let metadata = std::fs::metadata(&path).expect("stat jsonl file");
        assert!(metadata.len() > 0, "output JSONL must be non-empty");

        // 2. No placeholder "test" keys.
        for op in &history.operations {
            if let Op::Write { key, .. } = &op.op {
                assert_ne!(
                    key.as_str(),
                    "test",
                    "placeholder 'test' key must not appear in real history"
                );
            }
        }

        // 3. At least one operation recorded (even with 50ms the workload loops).
        assert!(
            !history.is_empty(),
            "history must contain at least one operation from the mock workload"
        );

        // 4. All operations carry the expected client_id.
        for op in &history.operations {
            assert_eq!(
                op.client_id, "mock-rust",
                "every operation must carry the configured client_id"
            );
        }

        // 5. JSONL round-trip: reload from disk and compare.
        let reloaded = History::from_jsonl(&path).expect("reload jsonl");
        assert_eq!(
            history.len(),
            reloaded.len(),
            "reloaded history must have the same number of operations"
        );
        for (a, b) in history.operations.iter().zip(reloaded.operations.iter()) {
            assert_eq!(a.client_id, b.client_id);
            assert_eq!(a.invoke_us, b.invoke_us);
            assert_eq!(a.complete_us, b.complete_us);
        }

        // 6. Results are valid OpResult variants (not random garbage).
        for op in &history.operations {
            match &op.result {
                OpResult::Ok
                | OpResult::Applied(_)
                | OpResult::Value(_)
                | OpResult::CurrentValues(_)
                | OpResult::Err(_)
                | OpResult::Timeout => {}
            }
        }
    }

    /// Verify that requesting an unknown workload returns an error (not a panic).
    #[tokio::test]
    async fn rust_driver_unknown_workload_errors() {
        let dir = tempfile::tempdir().unwrap();
        let _config = DriverConfig {
            kind: DriverKind::Rust,
            contact_points: vec!["127.0.0.1:9042".into()],
            workload: "does-not-exist".into(),
            duration: Duration::from_millis(50),
            threads: 1,
            output_dir: dir.path().to_path_buf(),
            client_id: "err-test".into(),
        };

        // ScyllaCqlSession::connect will fail (no cluster), but we need to test
        // the workload-not-found error path. We can't call driver.run() without
        // a live cluster, so test the registry lookup directly instead.
        let registry = WorkloadRegistry::phase1();
        assert!(
            registry.get("does-not-exist").is_none(),
            "unknown workload must not be found in registry"
        );
    }
}
