//! Two-phase DDL execution for Accord transactions.
//!
//! DDL operations (ALTER TABLE, CREATE INDEX, DROP TABLE, etc.) must be
//! coordinated with Accord transactions to prevent schema changes from
//! racing with in-flight DML. The protocol has two phases:
//!
//! ## Phase 1: DDL Marker
//!
//! A DDL marker transaction is submitted through Accord. New DML
//! transactions that conflict with the DDL marker must dep-wait on it.
//! This ensures all transactions see a consistent schema boundary.
//!
//! ## Phase 2: Schema Application
//!
//! After the DDL marker commits and all dep-waiting transactions resolve,
//! the actual schema change is applied via Raft. Once applied, new
//! transactions use the new schema.
//!
//! ## Timeout
//!
//! If the DDL marker does not commit within a configurable timeout
//! (default 30s), the DDL is aborted.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use ferrosa_common::accord::TxnId;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Table identifier for DDL scoping.
pub type TableId = String;

/// DDL operation type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DdlOperation {
    AlterTable { table: TableId, description: String },
    CreateIndex { table: TableId, column: String },
    DropTable { table: TableId },
    DropIndex { table: TableId, index_name: String },
}

impl DdlOperation {
    /// Target table of this DDL operation.
    pub fn table(&self) -> &str {
        match self {
            DdlOperation::AlterTable { table, .. } => table,
            DdlOperation::CreateIndex { table, .. } => table,
            DdlOperation::DropTable { table } => table,
            DdlOperation::DropIndex { table, .. } => table,
        }
    }
}

/// Status of a two-phase DDL operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlPhase {
    /// Phase 1: DDL marker submitted, waiting for commit.
    MarkerPending,
    /// Phase 1 complete: DDL marker committed, dep-waiting DML resolving.
    MarkerCommitted,
    /// Phase 2: Schema change being applied via Raft.
    Applying,
    /// DDL complete: schema change visible.
    Complete,
    /// DDL aborted due to timeout or conflict.
    Aborted,
}

/// Error from two-phase DDL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TwoPhaseDdlError {
    /// DDL timed out waiting for marker commit.
    Timeout { table: TableId, elapsed: Duration },
    /// Concurrent DDL on the same table.
    ConcurrentDdl { table: TableId },
    /// Table not found for DDL.
    TableNotFound { table: TableId },
}

impl std::fmt::Display for TwoPhaseDdlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TwoPhaseDdlError::Timeout { table, elapsed } => {
                write!(f, "DDL on {} timed out after {:?}", table, elapsed)
            }
            TwoPhaseDdlError::ConcurrentDdl { table } => {
                write!(f, "concurrent DDL already active on {}", table)
            }
            TwoPhaseDdlError::TableNotFound { table } => {
                write!(f, "table {} not found", table)
            }
        }
    }
}

impl std::error::Error for TwoPhaseDdlError {}

// ---------------------------------------------------------------------------
// DdlMarker
// ---------------------------------------------------------------------------

/// A DDL marker transaction that flows through Accord.
#[derive(Debug, Clone)]
pub struct DdlMarker {
    /// The Accord transaction ID for this marker.
    pub txn_id: TxnId,
    /// The DDL operation.
    pub operation: DdlOperation,
    /// Current phase.
    pub phase: DdlPhase,
    /// DML transactions that are dep-waiting on this marker.
    pub dep_waiters: HashSet<TxnId>,
}

// ---------------------------------------------------------------------------
// TwoPhaseDdlManager
// ---------------------------------------------------------------------------

/// Manages two-phase DDL operations coordinated with Accord.
pub struct TwoPhaseDdlManager {
    /// Active DDL operations by table.
    active_ddl: HashMap<TableId, DdlMarker>,
    /// DDL timeout.
    timeout: Duration,
    /// Completed schema changes (table -> version counter).
    schema_versions: HashMap<TableId, u64>,
}

impl TwoPhaseDdlManager {
    /// Default DDL timeout: 30 seconds.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

    /// Create a new manager.
    pub fn new() -> Self {
        Self {
            active_ddl: HashMap::new(),
            timeout: Self::DEFAULT_TIMEOUT,
            schema_versions: HashMap::new(),
        }
    }

    /// Create with a custom timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            active_ddl: HashMap::new(),
            timeout,
            schema_versions: HashMap::new(),
        }
    }

    /// Submit a DDL marker through Accord (Phase 1).
    ///
    /// Returns error if a DDL is already active on the same table.
    pub fn submit_marker(
        &mut self,
        txn_id: TxnId,
        operation: DdlOperation,
    ) -> Result<(), TwoPhaseDdlError> {
        let table = operation.table().to_string();
        if self.active_ddl.contains_key(&table) {
            return Err(TwoPhaseDdlError::ConcurrentDdl { table });
        }

        let marker = DdlMarker {
            txn_id,
            operation,
            phase: DdlPhase::MarkerPending,
            dep_waiters: HashSet::new(),
        };
        self.active_ddl.insert(table, marker);
        Ok(())
    }

    /// Check if a table has an active DDL marker.
    ///
    /// Returns the marker's TxnId if active, or None.
    pub fn active_marker(&self, table: &str) -> Option<TxnId> {
        self.active_ddl.get(table).map(|m| m.txn_id)
    }

    /// Register a DML transaction as dep-waiting on a DDL marker.
    ///
    /// This happens when a new DML transaction touches a table with
    /// an active DDL marker.
    pub fn register_dep_wait(&mut self, table: &str, dml_txn: TxnId) -> bool {
        if let Some(marker) = self.active_ddl.get_mut(table) {
            marker.dep_waiters.insert(dml_txn);
            true
        } else {
            false
        }
    }

    /// Mark the DDL marker as committed (Phase 1 complete).
    pub fn mark_committed(&mut self, table: &str) -> bool {
        if let Some(marker) = self.active_ddl.get_mut(table) {
            if marker.phase == DdlPhase::MarkerPending {
                marker.phase = DdlPhase::MarkerCommitted;
                return true;
            }
        }
        false
    }

    /// Begin applying the schema change (Phase 2).
    pub fn begin_apply(&mut self, table: &str) -> bool {
        if let Some(marker) = self.active_ddl.get_mut(table) {
            if marker.phase == DdlPhase::MarkerCommitted {
                marker.phase = DdlPhase::Applying;
                return true;
            }
        }
        false
    }

    /// Complete the DDL — schema change is now visible.
    pub fn complete(&mut self, table: &str) -> bool {
        if let Some(marker) = self.active_ddl.get_mut(table) {
            if marker.phase == DdlPhase::Applying {
                marker.phase = DdlPhase::Complete;
                // Bump schema version.
                *self.schema_versions.entry(table.to_string()).or_insert(0) += 1;
                return true;
            }
        }
        false
    }

    /// Remove a completed DDL from the active set.
    pub fn cleanup(&mut self, table: &str) -> Option<DdlMarker> {
        if let Some(marker) = self.active_ddl.get(table) {
            if marker.phase == DdlPhase::Complete || marker.phase == DdlPhase::Aborted {
                return self.active_ddl.remove(table);
            }
        }
        None
    }

    /// Abort a DDL due to timeout.
    pub fn abort_timeout(
        &mut self,
        table: &str,
        _elapsed: Duration,
    ) -> Result<(), TwoPhaseDdlError> {
        if let Some(marker) = self.active_ddl.get_mut(table) {
            marker.phase = DdlPhase::Aborted;
            Ok(())
        } else {
            Err(TwoPhaseDdlError::TableNotFound {
                table: table.to_string(),
            })
        }
    }

    /// Get the phase of an active DDL.
    pub fn phase(&self, table: &str) -> Option<DdlPhase> {
        self.active_ddl.get(table).map(|m| m.phase)
    }

    /// Get the schema version for a table.
    pub fn schema_version(&self, table: &str) -> u64 {
        self.schema_versions.get(table).copied().unwrap_or(0)
    }

    /// Get the dep-waiters for an active DDL.
    pub fn dep_waiters(&self, table: &str) -> Option<&HashSet<TxnId>> {
        self.active_ddl.get(table).map(|m| &m.dep_waiters)
    }

    /// DDL timeout duration.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Resolve a dep-waiter (DML txn completed).
    pub fn resolve_dep_waiter(&mut self, table: &str, dml_txn: &TxnId) -> bool {
        if let Some(marker) = self.active_ddl.get_mut(table) {
            marker.dep_waiters.remove(dml_txn)
        } else {
            false
        }
    }

    /// Check if all dep-waiters have resolved for a given table's DDL.
    pub fn all_dep_waiters_resolved(&self, table: &str) -> bool {
        self.active_ddl
            .get(table)
            .map(|m| m.dep_waiters.is_empty())
            .unwrap_or(true)
    }
}

impl Default for TwoPhaseDdlManager {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests — 4 tests for A7.6
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::accord::Timestamp;

    fn ts(time: u64) -> Timestamp {
        Timestamp {
            epoch: 0,
            time,
            seq: 0,
            node: 0,
        }
    }

    fn txn(time: u64) -> TxnId {
        TxnId(ts(time))
    }

    // -----------------------------------------------------------------------
    // Test 1: two_phase_ddl_dep_wait
    //   New DML txns dep-wait on DDL marker.
    // -----------------------------------------------------------------------

    #[test]
    fn two_phase_ddl_dep_wait() {
        let mut mgr = TwoPhaseDdlManager::new();
        let table = "ks.users";

        // Submit DDL marker.
        let ddl_txn = txn(1000);
        mgr.submit_marker(
            ddl_txn,
            DdlOperation::AlterTable {
                table: table.to_string(),
                description: "ADD COLUMN age int".to_string(),
            },
        )
        .expect("submit_marker");

        assert_eq!(mgr.phase(table), Some(DdlPhase::MarkerPending));
        assert_eq!(
            mgr.active_marker(table),
            Some(ddl_txn),
            "active marker must be the DDL txn"
        );

        // DML transactions arrive and must dep-wait on the DDL marker.
        let dml1 = txn(2000);
        let dml2 = txn(3000);
        assert!(
            mgr.register_dep_wait(table, dml1),
            "dml1 must register dep-wait"
        );
        assert!(
            mgr.register_dep_wait(table, dml2),
            "dml2 must register dep-wait"
        );

        // Verify dep-waiters are tracked.
        let waiters = mgr.dep_waiters(table).expect("waiters must exist");
        assert_eq!(waiters.len(), 2);
        assert!(waiters.contains(&dml1));
        assert!(waiters.contains(&dml2));

        // DML on a different table does NOT dep-wait.
        assert!(
            !mgr.register_dep_wait("ks.other", txn(4000)),
            "no DDL on ks.other — no dep-wait"
        );

        // Resolve dep-waiters.
        assert!(mgr.resolve_dep_waiter(table, &dml1));
        assert!(!mgr.all_dep_waiters_resolved(table));
        assert!(mgr.resolve_dep_waiter(table, &dml2));
        assert!(mgr.all_dep_waiters_resolved(table), "all waiters resolved");
    }

    // -----------------------------------------------------------------------
    // Test 2: two_phase_ddl_concurrent_dml
    //   Concurrent DML is handled correctly during DDL.
    // -----------------------------------------------------------------------

    #[test]
    fn two_phase_ddl_concurrent_dml() {
        let mut mgr = TwoPhaseDdlManager::new();
        let table = "ks.users";

        // Submit DDL marker.
        let ddl_txn = txn(1000);
        mgr.submit_marker(
            ddl_txn,
            DdlOperation::CreateIndex {
                table: table.to_string(),
                column: "email".to_string(),
            },
        )
        .expect("submit_marker");

        // Concurrent DDL on the same table is rejected.
        let ddl2 = txn(1500);
        let err = mgr
            .submit_marker(
                ddl2,
                DdlOperation::DropTable {
                    table: table.to_string(),
                },
            )
            .unwrap_err();
        assert!(
            matches!(err, TwoPhaseDdlError::ConcurrentDdl { .. }),
            "concurrent DDL must be rejected"
        );

        // DML txns dep-wait.
        let dml1 = txn(2000);
        let dml2 = txn(3000);
        let dml3 = txn(4000);
        mgr.register_dep_wait(table, dml1);
        mgr.register_dep_wait(table, dml2);
        mgr.register_dep_wait(table, dml3);

        // DDL marker commits.
        assert!(mgr.mark_committed(table));
        assert_eq!(mgr.phase(table), Some(DdlPhase::MarkerCommitted));

        // Resolve DML dep-waiters one by one.
        mgr.resolve_dep_waiter(table, &dml1);
        mgr.resolve_dep_waiter(table, &dml2);
        assert!(!mgr.all_dep_waiters_resolved(table), "dml3 still waiting");
        mgr.resolve_dep_waiter(table, &dml3);
        assert!(mgr.all_dep_waiters_resolved(table));

        // Phase 2: apply schema.
        assert!(mgr.begin_apply(table));
        assert_eq!(mgr.phase(table), Some(DdlPhase::Applying));

        // Complete.
        assert!(mgr.complete(table));
        assert_eq!(mgr.phase(table), Some(DdlPhase::Complete));

        // After completion, a new DDL on the same table can proceed.
        mgr.cleanup(table);
        let ddl3 = txn(5000);
        mgr.submit_marker(
            ddl3,
            DdlOperation::DropTable {
                table: table.to_string(),
            },
        )
        .expect("new DDL after cleanup must succeed");
    }

    // -----------------------------------------------------------------------
    // Test 3: two_phase_ddl_schema_change_visible
    //   Schema change is visible after completion.
    // -----------------------------------------------------------------------

    #[test]
    fn two_phase_ddl_schema_change_visible() {
        let mut mgr = TwoPhaseDdlManager::new();
        let table = "ks.users";

        // Initial schema version is 0.
        assert_eq!(mgr.schema_version(table), 0);

        // Submit and commit DDL marker.
        let ddl_txn = txn(1000);
        mgr.submit_marker(
            ddl_txn,
            DdlOperation::AlterTable {
                table: table.to_string(),
                description: "ADD COLUMN email text".to_string(),
            },
        )
        .unwrap();

        // Schema version unchanged during phase 1.
        assert_eq!(mgr.schema_version(table), 0);

        mgr.mark_committed(table);
        assert_eq!(mgr.schema_version(table), 0);

        // Phase 2: apply.
        mgr.begin_apply(table);
        assert_eq!(mgr.schema_version(table), 0, "not yet complete");

        // Complete — schema version bumps.
        mgr.complete(table);
        assert_eq!(
            mgr.schema_version(table),
            1,
            "schema version must be 1 after first DDL"
        );

        // Second DDL bumps again.
        mgr.cleanup(table);
        let ddl2 = txn(2000);
        mgr.submit_marker(
            ddl2,
            DdlOperation::CreateIndex {
                table: table.to_string(),
                column: "age".to_string(),
            },
        )
        .unwrap();
        mgr.mark_committed(table);
        mgr.begin_apply(table);
        mgr.complete(table);
        assert_eq!(
            mgr.schema_version(table),
            2,
            "schema version must be 2 after second DDL"
        );

        // Different table has its own version.
        assert_eq!(mgr.schema_version("ks.orders"), 0);
    }

    // -----------------------------------------------------------------------
    // Test 4: two_phase_ddl_abort_on_timeout
    //   DDL is aborted if timeout elapses.
    // -----------------------------------------------------------------------

    #[test]
    fn two_phase_ddl_abort_on_timeout() {
        let mut mgr = TwoPhaseDdlManager::with_timeout(Duration::from_secs(5));
        let table = "ks.users";

        // Submit DDL marker.
        let ddl_txn = txn(1000);
        mgr.submit_marker(
            ddl_txn,
            DdlOperation::DropTable {
                table: table.to_string(),
            },
        )
        .unwrap();

        assert_eq!(mgr.phase(table), Some(DdlPhase::MarkerPending));

        // Register some dep-waiters.
        mgr.register_dep_wait(table, txn(2000));
        mgr.register_dep_wait(table, txn(3000));

        // Simulate timeout (in production this would be driven by a timer).
        let elapsed = Duration::from_secs(6);
        mgr.abort_timeout(table, elapsed).expect("abort_timeout");

        assert_eq!(
            mgr.phase(table),
            Some(DdlPhase::Aborted),
            "DDL must be aborted after timeout"
        );

        // Schema version unchanged — DDL did not complete.
        assert_eq!(
            mgr.schema_version(table),
            0,
            "schema version must not change on aborted DDL"
        );

        // Attempting further phase transitions on an aborted DDL fails.
        assert!(!mgr.mark_committed(table), "cannot commit an aborted DDL");
        assert!(!mgr.begin_apply(table), "cannot apply an aborted DDL");
        assert!(!mgr.complete(table), "cannot complete an aborted DDL");

        // Cleanup removes the aborted DDL.
        let cleaned = mgr.cleanup(table);
        assert!(cleaned.is_some(), "aborted DDL should be cleanable");
        assert!(
            mgr.active_marker(table).is_none(),
            "no active marker after cleanup"
        );

        // Can now submit a new DDL on the same table.
        let ddl2 = txn(5000);
        mgr.submit_marker(
            ddl2,
            DdlOperation::DropTable {
                table: table.to_string(),
            },
        )
        .expect("new DDL after abort cleanup must succeed");
    }
}
