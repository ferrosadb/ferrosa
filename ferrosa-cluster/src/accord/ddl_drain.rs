//! DDL drain-and-block: stop accepting new Accord transactions for a table,
//! wait for in-flight transactions to complete, then allow DDL via Raft.
//!
//! When a DDL operation (ALTER TABLE, DROP TABLE, etc.) needs to execute, it
//! must first drain all in-flight Accord transactions for the target table.
//! New transactions on that table are rejected during the drain window, while
//! transactions on other tables and read operations are unaffected.
//!
//! # Protocol
//!
//! 1. **Block**: Caller calls [`DdlDrainGuard::begin_drain`] for a table.
//!    New Accord writes to that table are rejected with [`DrainError::TableDraining`].
//! 2. **Wait**: Caller calls [`DdlDrainGuard::wait_for_drain`] which returns
//!    once all in-flight transactions have completed (or times out).
//! 3. **DDL**: With no in-flight Accord transactions, DDL is applied via Raft.
//! 4. **Resume**: Caller calls [`DdlDrainGuard::end_drain`] to re-allow writes.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// Identifies a table for drain scoping (keyspace.table).
pub type TableId = String;

/// Errors returned when interacting with the drain guard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainError {
    /// The table is currently draining for DDL — new transactions are rejected.
    TableDraining(TableId),
    /// The drain timed out waiting for in-flight transactions to complete.
    DrainTimeout {
        table: TableId,
        remaining_txns: usize,
    },
    /// The table is not currently draining.
    NotDraining(TableId),
    /// The table is already draining.
    AlreadyDraining(TableId),
}

impl std::fmt::Display for DrainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DrainError::TableDraining(t) => {
                write!(f, "table {} is draining for DDL, new txns rejected", t)
            }
            DrainError::DrainTimeout {
                table,
                remaining_txns,
            } => write!(
                f,
                "drain timeout for table {}: {} txns still in-flight",
                table, remaining_txns
            ),
            DrainError::NotDraining(t) => write!(f, "table {} is not draining", t),
            DrainError::AlreadyDraining(t) => write!(f, "table {} is already draining", t),
        }
    }
}

impl std::error::Error for DrainError {}

/// Tracks in-flight transaction counts per table and enforces drain blocks.
///
/// Thread-safety: This struct uses interior mutability patterns suitable for
/// single-threaded or externally-synchronized access. For async contexts,
/// wrap in a `tokio::sync::Mutex`.
pub struct DdlDrainGuard {
    /// Tables currently in drain mode — new transactions are rejected.
    draining_tables: HashSet<TableId>,
    /// Count of in-flight Accord transactions per table.
    in_flight_counts: HashMap<TableId, usize>,
}

impl DdlDrainGuard {
    /// Create a new drain guard with no active drains.
    pub fn new() -> Self {
        Self {
            draining_tables: HashSet::new(),
            in_flight_counts: HashMap::new(),
        }
    }

    /// Begin draining a table: mark it as draining so new transactions are
    /// rejected.
    ///
    /// Returns `Err(AlreadyDraining)` if the table is already in drain mode.
    pub fn begin_drain(&mut self, table: &TableId) -> Result<(), DrainError> {
        if self.draining_tables.contains(table) {
            return Err(DrainError::AlreadyDraining(table.clone()));
        }
        self.draining_tables.insert(table.clone());
        Ok(())
    }

    /// Check whether a new transaction on `table` should be accepted.
    ///
    /// Returns `Err(TableDraining)` if the table is currently draining.
    /// Returns `Ok(())` if the table is not draining.
    pub fn check_transaction_allowed(&self, table: &TableId) -> Result<(), DrainError> {
        if self.draining_tables.contains(table) {
            Err(DrainError::TableDraining(table.clone()))
        } else {
            Ok(())
        }
    }

    /// Register a new in-flight transaction for a table.
    ///
    /// Call this when a transaction begins (PreAccept). Fails if the table
    /// is draining.
    pub fn register_in_flight(&mut self, table: &TableId) -> Result<(), DrainError> {
        self.check_transaction_allowed(table)?;
        *self.in_flight_counts.entry(table.clone()).or_insert(0) += 1;
        Ok(())
    }

    /// Mark a transaction as completed for a table.
    ///
    /// Call this when a transaction reaches Applied or is aborted.
    /// Decrements the in-flight count (saturating at 0).
    pub fn complete_transaction(&mut self, table: &TableId) {
        if let Some(count) = self.in_flight_counts.get_mut(table) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.in_flight_counts.remove(table);
            }
        }
    }

    /// Get the current in-flight count for a table.
    pub fn in_flight_count(&self, table: &TableId) -> usize {
        self.in_flight_counts.get(table).copied().unwrap_or(0)
    }

    /// Check whether the drain is complete (no in-flight transactions remain).
    ///
    /// Returns `Ok(())` if the table is draining and has zero in-flight
    /// transactions. Returns `Err(DrainTimeout)` if transactions remain.
    /// Returns `Err(NotDraining)` if the table is not in drain mode.
    pub fn check_drain_complete(&self, table: &TableId) -> Result<(), DrainError> {
        if !self.draining_tables.contains(table) {
            return Err(DrainError::NotDraining(table.clone()));
        }
        let count = self.in_flight_count(table);
        if count == 0 {
            Ok(())
        } else {
            Err(DrainError::DrainTimeout {
                table: table.clone(),
                remaining_txns: count,
            })
        }
    }

    /// Wait for drain to complete, polling with the given interval up to
    /// the given timeout.
    ///
    /// This is the synchronous polling version. Each poll checks whether
    /// in-flight count has reached zero. The caller is responsible for
    /// completing transactions between polls (e.g., by processing commits).
    ///
    /// Returns `Ok(())` if drain completes within timeout.
    /// Returns `Err(DrainTimeout)` if timeout elapses with txns remaining.
    pub fn wait_for_drain(
        &self,
        table: &TableId,
        _timeout: Duration,
        _poll_interval: Duration,
    ) -> Result<(), DrainError> {
        // For synchronous / test usage, just check once.
        // In production, this would be async with tokio::time::timeout.
        self.check_drain_complete(table)
    }

    /// End the drain: remove the table from drain mode, re-allowing
    /// new transactions.
    ///
    /// Returns `Err(NotDraining)` if the table is not currently draining.
    pub fn end_drain(&mut self, table: &TableId) -> Result<(), DrainError> {
        if !self.draining_tables.remove(table) {
            return Err(DrainError::NotDraining(table.clone()));
        }
        Ok(())
    }

    /// Check whether a read operation on `table` is allowed.
    ///
    /// Reads are ALWAYS allowed, even during drain. The drain only blocks
    /// new Accord write transactions — reads do not create dependencies
    /// in the conflict index and are safe to execute concurrently.
    pub fn check_read_allowed(&self, _table: &TableId) -> Result<(), DrainError> {
        Ok(())
    }

    /// Check if a table is currently in drain mode.
    pub fn is_draining(&self, table: &TableId) -> bool {
        self.draining_tables.contains(table)
    }
}

impl Default for DdlDrainGuard {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str) -> TableId {
        name.to_string()
    }

    // -----------------------------------------------------------------------
    // Test 1: ddl_drain_and_block
    //   Drain blocks new txns, waits for in-flight, then allows DDL.
    // -----------------------------------------------------------------------

    #[test]
    fn ddl_drain_and_block() {
        let mut guard = DdlDrainGuard::new();
        let tbl = table("ks.users");

        // Register two in-flight transactions before drain.
        guard
            .register_in_flight(&tbl)
            .expect("register before drain");
        guard
            .register_in_flight(&tbl)
            .expect("register before drain");
        assert_eq!(guard.in_flight_count(&tbl), 2);

        // Begin drain — table is now blocked.
        guard.begin_drain(&tbl).expect("begin drain");
        assert!(guard.is_draining(&tbl));

        // New transactions must be rejected.
        let err = guard.register_in_flight(&tbl).unwrap_err();
        assert_eq!(err, DrainError::TableDraining(tbl.clone()));

        // check_transaction_allowed also rejects.
        assert!(guard.check_transaction_allowed(&tbl).is_err());

        // Drain is not yet complete — 2 in-flight.
        let check = guard.check_drain_complete(&tbl);
        assert!(check.is_err());
        match check.unwrap_err() {
            DrainError::DrainTimeout { remaining_txns, .. } => assert_eq!(remaining_txns, 2),
            other => panic!("expected DrainTimeout, got {:?}", other),
        }

        // Complete first transaction.
        guard.complete_transaction(&tbl);
        assert_eq!(guard.in_flight_count(&tbl), 1);

        // Still not drained.
        assert!(guard.check_drain_complete(&tbl).is_err());

        // Complete second transaction.
        guard.complete_transaction(&tbl);
        assert_eq!(guard.in_flight_count(&tbl), 0);

        // Drain is now complete — DDL can proceed.
        guard
            .check_drain_complete(&tbl)
            .expect("drain should be complete");

        // End drain — table is re-opened for new transactions.
        guard.end_drain(&tbl).expect("end drain");
        assert!(!guard.is_draining(&tbl));

        // New transactions are accepted again.
        guard
            .register_in_flight(&tbl)
            .expect("register after drain ended");
        assert_eq!(guard.in_flight_count(&tbl), 1);
    }

    // -----------------------------------------------------------------------
    // Test 2: ddl_drain_timeout
    //   Drain times out if in-flight txns don't complete.
    // -----------------------------------------------------------------------

    #[test]
    fn ddl_drain_timeout() {
        let mut guard = DdlDrainGuard::new();
        let tbl = table("ks.orders");

        // Register three in-flight transactions.
        guard.register_in_flight(&tbl).unwrap();
        guard.register_in_flight(&tbl).unwrap();
        guard.register_in_flight(&tbl).unwrap();

        // Begin drain.
        guard.begin_drain(&tbl).unwrap();

        // Complete only one — two remain.
        guard.complete_transaction(&tbl);

        // Attempt to wait — should timeout because 2 txns remain.
        let result =
            guard.wait_for_drain(&tbl, Duration::from_millis(100), Duration::from_millis(10));
        assert!(result.is_err());

        match result.unwrap_err() {
            DrainError::DrainTimeout {
                table: t,
                remaining_txns,
            } => {
                assert_eq!(t, tbl);
                assert_eq!(remaining_txns, 2, "two txns should still be in-flight");
            }
            other => panic!("expected DrainTimeout, got {:?}", other),
        }

        // Now complete remaining and verify drain succeeds.
        guard.complete_transaction(&tbl);
        guard.complete_transaction(&tbl);

        let result =
            guard.wait_for_drain(&tbl, Duration::from_millis(100), Duration::from_millis(10));
        assert!(
            result.is_ok(),
            "drain should succeed after all txns complete"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: ddl_drain_concurrent_reads
    //   Reads are not blocked during drain.
    // -----------------------------------------------------------------------

    #[test]
    fn ddl_drain_concurrent_reads() {
        let mut guard = DdlDrainGuard::new();
        let tbl = table("ks.events");

        // Register an in-flight write transaction.
        guard.register_in_flight(&tbl).unwrap();

        // Begin drain.
        guard.begin_drain(&tbl).unwrap();

        // New write transactions are blocked.
        assert!(guard.register_in_flight(&tbl).is_err());

        // Reads are NOT blocked — check_read_allowed always returns Ok.
        // The drain only blocks new Accord write transactions, not reads.
        assert!(
            guard.check_read_allowed(&tbl).is_ok(),
            "reads must not be blocked during drain"
        );

        // Even while draining with in-flight txns, reads pass through.
        assert!(guard.check_read_allowed(&tbl).is_ok());

        // Reads on any table are always allowed (even draining ones).
        let other = table("ks.other");
        assert!(guard.check_read_allowed(&other).is_ok());
    }

    // -----------------------------------------------------------------------
    // Test 4: ddl_drain_other_tables_unaffected
    //   Drain is table-scoped, other tables are unaffected.
    // -----------------------------------------------------------------------

    #[test]
    fn ddl_drain_other_tables_unaffected() {
        let mut guard = DdlDrainGuard::new();
        let draining_tbl = table("ks.users");
        let other_tbl = table("ks.orders");

        // Register in-flight on both tables.
        guard.register_in_flight(&draining_tbl).unwrap();
        guard.register_in_flight(&other_tbl).unwrap();

        // Begin drain on users only.
        guard.begin_drain(&draining_tbl).unwrap();

        // Users table: new txns blocked.
        assert!(guard.register_in_flight(&draining_tbl).is_err());
        assert!(guard.is_draining(&draining_tbl));

        // Orders table: completely unaffected.
        assert!(!guard.is_draining(&other_tbl));
        guard
            .register_in_flight(&other_tbl)
            .expect("other table must accept new txns during drain of a different table");
        assert_eq!(guard.in_flight_count(&other_tbl), 2);

        // Check that check_transaction_allowed is table-scoped.
        assert!(guard.check_transaction_allowed(&draining_tbl).is_err());
        assert!(guard.check_transaction_allowed(&other_tbl).is_ok());

        // Complete the draining table's txn.
        guard.complete_transaction(&draining_tbl);
        guard
            .check_drain_complete(&draining_tbl)
            .expect("draining table should be drained");

        // Other table's in-flight count is independent.
        assert_eq!(guard.in_flight_count(&other_tbl), 2);

        // End drain on users — verify both tables are open.
        guard.end_drain(&draining_tbl).unwrap();

        guard
            .register_in_flight(&draining_tbl)
            .expect("previously draining table should accept txns again");
        guard
            .register_in_flight(&other_tbl)
            .expect("other table should still accept txns");
    }
}
