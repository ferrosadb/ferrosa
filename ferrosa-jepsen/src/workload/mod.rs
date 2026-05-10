pub mod bank;
pub mod forward_probe;
pub mod lwt;
pub mod register;

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

use crate::history::{History, HistoryRecorder};

/// A CQL session abstraction (will be backed by cdrs-tokio or mock).
/// For now, a trait that workloads program against.
#[async_trait]
pub trait CqlSession: Send + Sync {
    async fn execute(&self, query: &str) -> Result<Vec<Vec<(String, String)>>>;
}

/// A workload that generates operations and checks invariants.
#[async_trait]
pub trait Workload: Send + Sync {
    /// Human-readable name.
    fn name(&self) -> &str;

    /// Set up the schema (CREATE TABLE, etc).
    async fn setup(&self, session: &dyn CqlSession) -> Result<()>;

    /// Generate operations for the given duration.
    /// Records operations into the HistoryRecorder.
    async fn run(
        &self,
        session: &dyn CqlSession,
        recorder: &mut HistoryRecorder,
        duration: Duration,
    ) -> Result<()>;

    /// Check the history for correctness invariants.
    fn check_invariant(&self, history: &History) -> Result<()>;
}

/// Registry of all available workloads.
pub struct WorkloadRegistry {
    workloads: Vec<Box<dyn Workload>>,
}

impl Default for WorkloadRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkloadRegistry {
    pub fn new() -> Self {
        Self {
            workloads: Vec::new(),
        }
    }

    pub fn register(&mut self, w: Box<dyn Workload>) {
        self.workloads.push(w);
    }

    pub fn get(&self, name: &str) -> Option<&dyn Workload> {
        self.workloads
            .iter()
            .find(|w| w.name() == name)
            .map(|b| b.as_ref())
    }

    pub fn names(&self) -> Vec<String> {
        self.workloads
            .iter()
            .map(|w| w.name().to_string())
            .collect()
    }

    /// Create registry with all Phase 1 workloads.
    pub fn phase1() -> Self {
        let mut reg = Self::new();
        reg.register(Box::new(register::RegisterWorkload));
        reg.register(Box::new(bank::BankWorkload));
        for wl in lwt::all_lwt_workloads() {
            reg.register(wl);
        }
        // Sprint 2 workloads — exercise the membership / forwarding bug
        // classes the structural-invariant checker is designed to catch.
        reg.register(Box::new(forward_probe::ForwardProbeWorkload));
        reg
    }
}

/// A `CqlSession` that accepts any query and returns empty results.
///
/// Used in unit tests and in-process orchestrator runs where a real CQL
/// cluster is not available. Queries execute instantly without I/O.
pub struct MockCqlSession;

#[async_trait]
impl CqlSession for MockCqlSession {
    async fn execute(&self, _query: &str) -> Result<Vec<Vec<(String, String)>>> {
        Ok(vec![])
    }
}

#[cfg(test)]
pub mod testutil {
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use super::CqlSession;

    /// Minimal mock CQL session for unit tests.
    ///
    /// Behaviour rules (applied in order):
    /// - DDL (CREATE KEYSPACE / TABLE / TYPE): empty result set.
    /// - Seed inserts without IF: empty result set.
    /// - SELECT queries: return a single row whose column name is derived from
    ///   the first column in the SELECT list, with value "1000".
    /// - LWT mutations (queries containing "IF"): return `[applied]=true` on
    ///   the first call, then alternate true/false to simulate contention.
    /// - Other mutations (UPDATE … without IF): empty result set.
    pub struct MockCqlSession {
        /// Flips between true/false for LWT applied responses.
        lwt_toggle: Arc<AtomicBool>,
    }

    impl Default for MockCqlSession {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MockCqlSession {
        pub fn new() -> Self {
            Self {
                lwt_toggle: Arc::new(AtomicBool::new(true)),
            }
        }
    }

    #[async_trait]
    impl CqlSession for MockCqlSession {
        async fn execute(&self, query: &str) -> Result<Vec<Vec<(String, String)>>> {
            let q = query.trim_start().to_ascii_uppercase();

            // DDL: CREATE KEYSPACE / TABLE / TYPE
            if q.starts_with("CREATE") {
                return Ok(vec![]);
            }

            // SELECT: return a single-row, single-column result.
            // Column name is taken from the first token after SELECT and before FROM.
            if q.starts_with("SELECT") {
                // Extract the column name from the original (case-preserved) query.
                let col = query
                    .trim_start()
                    .split(' ')
                    .nth(1)
                    .unwrap_or("val")
                    .trim_end_matches(',');
                return Ok(vec![vec![(col.to_string(), "1000".to_string())]]);
            }

            // LWT: any mutation containing "IF" (at word boundary in upper-case form).
            if q.contains(" IF ") || q.ends_with(" IF EXISTS") || q.ends_with(" IF NOT EXISTS") {
                let applied = self.lwt_toggle.fetch_xor(true, Ordering::Relaxed);
                return Ok(vec![vec![("[applied]".to_string(), applied.to_string())]]);
            }

            // Plain mutation (INSERT without IF, UPDATE without IF): success, no rows.
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_registry_phase1() {
        let reg = WorkloadRegistry::phase1();
        // register + bank + 16 LWT + Sprint 2 workloads (forward-probe).
        assert_eq!(reg.names().len(), 19);
        assert!(reg.get("register").is_some());
        assert!(reg.get("bank").is_some());
        assert!(reg.get("lwt-1-insert-if-not-exists").is_some());
        assert!(reg.get("lwt-16-multi-statement").is_some());
        assert!(reg.get("forward-probe").is_some());
    }

    #[tokio::test]
    async fn mock_session_accepts_any_query() {
        let session = MockCqlSession;
        let result = session.execute("SELECT * FROM system.local").await.unwrap();
        assert!(result.is_empty());
    }

    // -----------------------------------------------------------------------
    // JP-001: Workload operation generation unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn workload_registry_get_returns_none_for_unknown() {
        let reg = WorkloadRegistry::phase1();
        assert!(reg.get("nonexistent-workload").is_none());
    }

    #[test]
    fn workload_registry_empty_has_no_workloads() {
        let reg = WorkloadRegistry::new();
        assert!(reg.names().is_empty());
        assert!(reg.get("register").is_none());
    }

    #[test]
    fn workload_registry_register_and_retrieve() {
        let mut reg = WorkloadRegistry::new();
        reg.register(Box::new(register::RegisterWorkload));
        assert_eq!(reg.names(), vec!["register"]);
        assert!(reg.get("register").is_some());
        assert_eq!(reg.get("register").unwrap().name(), "register");
    }

    #[test]
    fn workload_registry_phase1_names_are_unique() {
        let reg = WorkloadRegistry::phase1();
        let names = reg.names();
        let mut deduped = names.clone();
        deduped.sort();
        deduped.dedup();
        assert_eq!(
            names.len(),
            deduped.len(),
            "phase1 registry must have unique workload names"
        );
    }

    #[test]
    fn workload_registry_phase1_all_names_non_empty() {
        let reg = WorkloadRegistry::phase1();
        for name in reg.names() {
            assert!(!name.is_empty(), "workload names must not be empty strings");
        }
    }

    #[test]
    fn workload_registry_phase1_has_register_and_bank() {
        let reg = WorkloadRegistry::phase1();
        let names = reg.names();
        assert!(
            names.contains(&"register".to_string()),
            "phase1 must include register workload"
        );
        assert!(
            names.contains(&"bank".to_string()),
            "phase1 must include bank workload"
        );
    }

    #[test]
    fn workload_registry_phase1_has_all_lwt_patterns() {
        let reg = WorkloadRegistry::phase1();
        let names = reg.names();
        // Verify all 16 LWT patterns are present.
        let lwt_count = names.iter().filter(|n| n.starts_with("lwt-")).count();
        assert_eq!(lwt_count, 16, "phase1 must include all 16 LWT workloads");
    }

    #[test]
    fn workload_registry_phase1_lwt_names_follow_pattern() {
        let reg = WorkloadRegistry::phase1();
        let names = reg.names();
        let lwt_names: Vec<&String> = names.iter().filter(|n| n.starts_with("lwt-")).collect();
        // Each LWT name should start with "lwt-N-" where N is 1..=16.
        for name in &lwt_names {
            let parts: Vec<&str> = name.splitn(3, '-').collect();
            assert!(
                parts.len() >= 3,
                "LWT name should have format lwt-N-desc: {name}"
            );
            let num: usize = parts[1]
                .parse()
                .unwrap_or_else(|_| panic!("LWT name second segment should be a number: {name}"));
            assert!(
                (1..=16).contains(&num),
                "LWT pattern number should be 1..=16, got {num} in {name}"
            );
        }
    }

    /// All Phase 1 workloads should successfully execute setup + run against the
    /// mock CQL session and produce a history (possibly empty for very short runs).
    #[tokio::test]
    async fn all_phase1_workloads_run_against_mock() {
        use std::time::Duration;
        use testutil::MockCqlSession;

        let reg = WorkloadRegistry::phase1();
        let session = MockCqlSession::new();

        for name in reg.names() {
            let wl = reg.get(&name).unwrap();
            wl.setup(&session)
                .await
                .unwrap_or_else(|e| panic!("setup failed for workload '{name}': {e}"));

            let mut recorder = crate::history::HistoryRecorder::new("test");
            // 200ms (was 20ms): under workspace-wide parallel test load, a
            // 20ms slot is too tight on slower runners and the workload's
            // run loop occasionally returns Err("deadline exceeded") on
            // setup queries. 200ms preserves the test's "short run" intent
            // while removing the timing race.
            wl.run(&session, &mut recorder, Duration::from_millis(200))
                .await
                .unwrap_or_else(|e| panic!("run failed for workload '{name}': {e}"));

            let history = recorder.finish();
            // Most workloads should produce at least one operation in 20ms.
            // We don't assert non-empty because some workloads may need more
            // than one iteration to get through their setup queries.
            // Instead, verify that the history is structurally valid.
            for op in &history.operations {
                assert!(
                    op.invoke_us <= op.complete_us,
                    "workload '{name}' produced operation with invoke > complete"
                );
                assert!(
                    !op.client_id.is_empty(),
                    "workload '{name}' produced operation with empty client_id"
                );
            }
        }
    }
}
