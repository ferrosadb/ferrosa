//! Session-level state for CQL connections.
//!
//! Tracks transaction state (Accord transactions) and validates statement
//! transitions. Nested transactions are rejected. DDL inside transactions
//! is rejected.

use crate::ast::Statement;
use crate::error::CqlError;
use ferrosa_storage::{BatchOp, StorageEngine};

/// Transaction state for a CQL session.
///
/// Tracks whether the session is currently inside an Accord transaction
/// and validates incoming statements against that state.
#[derive(Debug)]
pub struct TransactionState {
    in_transaction: bool,
}

impl TransactionState {
    /// Create a new transaction state (not in a transaction).
    pub fn new() -> Self {
        Self {
            in_transaction: false,
        }
    }

    /// Returns true if the session is currently inside a transaction.
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Validate a statement against the current transaction state and
    /// update the state if appropriate.
    ///
    /// Returns an error if:
    /// - BEGIN TRANSACTION is issued while already in a transaction (nested)
    /// - A DDL statement is issued while inside a transaction
    /// - COMMIT or ROLLBACK is issued while not in a transaction
    pub fn validate_and_transition(&mut self, stmt: &Statement) -> Result<(), CqlError> {
        match stmt {
            Statement::BeginTransaction => {
                if self.in_transaction {
                    return Err(CqlError::Invalid(
                        "nested transactions are not supported".to_string(),
                    ));
                }
                self.in_transaction = true;
                Ok(())
            }
            Statement::Commit => {
                if !self.in_transaction {
                    return Err(CqlError::Invalid(
                        "COMMIT outside of a transaction".to_string(),
                    ));
                }
                self.in_transaction = false;
                Ok(())
            }
            Statement::Rollback => {
                if !self.in_transaction {
                    return Err(CqlError::Invalid(
                        "ROLLBACK outside of a transaction".to_string(),
                    ));
                }
                self.in_transaction = false;
                Ok(())
            }
            other => {
                if self.in_transaction && other.is_ddl() {
                    return Err(CqlError::Invalid(
                        "DDL statements are not permitted inside a transaction".to_string(),
                    ));
                }
                Ok(())
            }
        }
    }
}

impl Default for TransactionState {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-connection **explicit-transaction state machine** (spec URS-QEC-B02).
///
/// This is the connection-level machine that backs Bolt
/// `BEGIN` / `RUN` / `COMMIT` / `ROLLBACK` (and, in future, an analogous CQL
/// explicit-transaction surface). It deliberately separates *staging* from
/// *durability*:
///
/// * [`begin`](Self::begin) opens a transaction (assigns a `tx_id`, defers all
///   execution); a second `begin` while open FAILS LOUD (no nested tx).
/// * [`stage`](Self::stage) queues a [`BatchOp`] produced by a `RUN`/`PULL`
///   write **without** touching durable storage. Staging outside an open tx
///   FAILS LOUD.
/// * Reads inside the tx see the connection's own staged writes via
///   [`staged_ops`](Self::staged_ops).
/// * [`commit`](Self::commit) materializes a [`BatchTxn`] from the engine's
///   `begin_batch()`, stages every queued op onto it, and calls
///   `BatchTxn::commit` — an **atomic, durable, all-or-nothing** apply. On any
///   engine error it returns `Err` and the connection has persisted *nothing*
///   (URS-QEC-X01: never ack a transaction we didn't persist).
/// * [`rollback`](Self::rollback) discards the staged ops (`BatchTxn::abort`
///   semantics — nothing was ever written).
///
/// Staging holds an owned `Vec<BatchOp>` rather than a live `BatchTxn` so the
/// machine does not borrow the engine across `RUN` round-trips; the borrowing
/// `BatchTxn` is materialized only for the duration of `commit`.
#[derive(Debug, Default)]
pub struct ConnTxn {
    /// `Some(tx_id)` while a transaction is open; `None` otherwise.
    open: Option<u64>,
    /// Writes staged by `RUN`/`PULL` since `begin`, in submission order.
    staged: Vec<BatchOp>,
}

impl ConnTxn {
    /// A fresh connection with no open transaction.
    pub fn new() -> Self {
        Self::default()
    }

    /// `true` while a transaction is open.
    pub fn is_open(&self) -> bool {
        self.open.is_some()
    }

    /// The open transaction's id, if any.
    pub fn tx_id(&self) -> Option<u64> {
        self.open
    }

    /// Open an explicit transaction. FAILS LOUD on a nested `BEGIN`.
    pub fn begin(&mut self, tx_id: u64) -> Result<(), CqlError> {
        if self.open.is_some() {
            return Err(CqlError::Invalid(
                "BEGIN received while a transaction is already open (nested transactions \
                 are not supported)"
                    .to_string(),
            ));
        }
        self.open = Some(tx_id);
        self.staged.clear();
        Ok(())
    }

    /// Stage a write onto the open transaction's batch. FAILS LOUD if no
    /// transaction is open (a `RUN` write must not silently escape the tx).
    pub fn stage(&mut self, op: BatchOp) -> Result<(), CqlError> {
        if self.open.is_none() {
            return Err(CqlError::Invalid(
                "write staged with no open explicit transaction".to_string(),
            ));
        }
        self.staged.push(op);
        Ok(())
    }

    /// The connection's own staged writes (read-your-own-writes inside the tx).
    pub fn staged_ops(&self) -> &[BatchOp] {
        &self.staged
    }

    /// Atomically commit the staged batch via the storage primitive.
    ///
    /// Opens a [`BatchTxn`] from `engine.begin_batch()`, stages every queued op,
    /// and calls `BatchTxn::commit` (single atomic, durable apply). On engine
    /// error the connection state is still reset (the tx is over) but the `Err`
    /// is returned so the caller emits a Bolt `FAILURE` — the transaction is
    /// **not** acknowledged as committed (URS-QEC-X01, fail-loud, no partial).
    ///
    /// FAILS LOUD if no transaction is open.
    pub fn commit(&mut self, engine: &StorageEngine) -> Result<(), CqlError> {
        if self.open.is_none() {
            return Err(CqlError::Invalid(
                "COMMIT received with no open explicit transaction".to_string(),
            ));
        }
        // Take ownership of the staged ops and close the tx regardless of
        // outcome — a commit attempt ends the transaction either way.
        let ops = std::mem::take(&mut self.staged);
        self.open = None;

        let mut batch = engine.begin_batch();
        for op in ops {
            batch.stage(op);
        }
        // BatchTxn::commit is atomic + durable + all-or-nothing; propagate any
        // error so the caller never acks a transaction that did not persist.
        batch.commit()?;
        Ok(())
    }

    /// Abort the open transaction, discarding all staged writes. Nothing was
    /// ever written, so this is pure in-memory cleanup. FAILS LOUD if no
    /// transaction is open.
    pub fn rollback(&mut self) -> Result<(), CqlError> {
        if self.open.is_none() {
            return Err(CqlError::Invalid(
                "ROLLBACK received with no open explicit transaction".to_string(),
            ));
        }
        self.staged.clear();
        self.open = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    #[test]
    fn transition_begin_commit() {
        let mut state = TransactionState::new();
        assert!(!state.in_transaction());

        state
            .validate_and_transition(&Statement::BeginTransaction)
            .unwrap();
        assert!(state.in_transaction());

        state.validate_and_transition(&Statement::Commit).unwrap();
        assert!(!state.in_transaction());
    }

    #[test]
    fn transition_begin_rollback() {
        let mut state = TransactionState::new();

        state
            .validate_and_transition(&Statement::BeginTransaction)
            .unwrap();
        assert!(state.in_transaction());

        state.validate_and_transition(&Statement::Rollback).unwrap();
        assert!(!state.in_transaction());
    }

    #[test]
    fn nested_begin_rejected() {
        let mut state = TransactionState::new();
        state
            .validate_and_transition(&Statement::BeginTransaction)
            .unwrap();

        let err = state
            .validate_and_transition(&Statement::BeginTransaction)
            .unwrap_err();
        assert!(err.to_string().contains("nested"));
    }

    #[test]
    fn ddl_in_transaction_rejected() {
        let mut state = TransactionState::new();
        state
            .validate_and_transition(&Statement::BeginTransaction)
            .unwrap();

        let ddl = Statement::CreateTable(CreateTableStatement {
            keyspace: Some("ks".into()),
            name: "t".into(),
            columns: vec![("k".into(), CqlTypeName::Simple("int".into()))],
            partition_key: vec!["k".into()],
            clustering_key: vec![],
            if_not_exists: false,
            table_options: vec![],
            extensions: None,
        });
        let err = state.validate_and_transition(&ddl).unwrap_err();
        assert!(err.to_string().contains("DDL"));
    }

    #[test]
    fn dml_in_transaction_allowed() {
        let mut state = TransactionState::new();
        state
            .validate_and_transition(&Statement::BeginTransaction)
            .unwrap();

        let dml = Statement::Insert(InsertStatement {
            keyspace: Some("ks".into()),
            table: "t".into(),
            columns: vec!["k".into()],
            values: vec![Term::IntegerLiteral(1)],
            if_not_exists: false,
            using_timestamp: None,
            using_ttl: None,
        });
        state.validate_and_transition(&dml).unwrap();
    }

    #[test]
    fn commit_outside_transaction_rejected() {
        let mut state = TransactionState::new();
        let err = state
            .validate_and_transition(&Statement::Commit)
            .unwrap_err();
        assert!(err.to_string().contains("COMMIT outside"));
    }

    #[test]
    fn rollback_outside_transaction_rejected() {
        let mut state = TransactionState::new();
        let err = state
            .validate_and_transition(&Statement::Rollback)
            .unwrap_err();
        assert!(err.to_string().contains("ROLLBACK outside"));
    }
}
