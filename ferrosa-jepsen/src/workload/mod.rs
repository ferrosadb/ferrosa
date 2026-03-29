pub mod bank;
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
        reg
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
                return Ok(vec![vec![(
                    "[applied]".to_string(),
                    applied.to_string(),
                )]]);
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
        assert_eq!(reg.names().len(), 18); // register + bank + 16 LWT
        assert!(reg.get("register").is_some());
        assert!(reg.get("bank").is_some());
        assert!(reg.get("lwt-1-insert-if-not-exists").is_some());
        assert!(reg.get("lwt-16-multi-statement").is_some());
    }
}
