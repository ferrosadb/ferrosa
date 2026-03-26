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
