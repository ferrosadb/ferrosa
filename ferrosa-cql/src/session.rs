//! Session-level state for CQL connections.
//!
//! Tracks transaction state (Accord transactions) and validates statement
//! transitions. Nested transactions are rejected. DDL inside transactions
//! is rejected.

use std::time::{Duration, Instant};

use crate::ast::Statement;
use crate::error::CqlError;
use ferrosa_storage::accord::{CommitOutcome, TransactionCommitter, TransactionWrite};
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
/// * [`commit`](Self::commit) materializes a `BatchTxn` from the engine's
///   `begin_batch()`, stages every queued op onto it, and calls
///   `BatchTxn::commit` — an **atomic, durable, all-or-nothing** apply. On any
///   engine error it returns `Err` and the connection has persisted *nothing*
///   (URS-QEC-X01: never ack a transaction we didn't persist).
/// * [`rollback`](Self::rollback) discards the staged ops (`BatchTxn::abort`
///   semantics — nothing was ever written).
///
/// ## Timeout enforcement (URS-QEC-B03)
///
/// A transaction may be opened with a deadline via
/// [`begin_with_timeout`](Self::begin_with_timeout) (Bolt's `tx_timeout`
/// metadata). Once the deadline passes, the next [`stage`](Self::stage) or
/// [`commit`](Self::commit) **aborts** the transaction (discards every staged
/// write, closes the tx) and FAILS LOUD with [`CqlError::TransactionTimeout`].
/// A timed-out `COMMIT` therefore persists *nothing* — the server never acks a
/// transaction whose budget it blew (URS-QEC-X01, fail-loud, never fake). A
/// transaction opened via plain [`begin`](Self::begin) has no deadline and
/// never expires.
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
    /// Per-transaction deadline. `Some((deadline, budget))` when the open tx was
    /// started with a timeout; `None` for an unbounded transaction.
    deadline: Option<(Instant, Duration)>,
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

    /// Open an explicit transaction with **no timeout** (unbounded). FAILS LOUD
    /// on a nested `BEGIN`.
    pub fn begin(&mut self, tx_id: u64) -> Result<(), CqlError> {
        self.begin_inner(tx_id, None)
    }

    /// Open an explicit transaction with a per-transaction `timeout`
    /// (URS-QEC-B03; Bolt `tx_timeout`). After `timeout` elapses, the next
    /// `stage`/`commit` aborts the tx and FAILS LOUD with
    /// [`CqlError::TransactionTimeout`]. FAILS LOUD on a nested `BEGIN`.
    pub fn begin_with_timeout(&mut self, tx_id: u64, timeout: Duration) -> Result<(), CqlError> {
        self.begin_inner(tx_id, Some(timeout))
    }

    fn begin_inner(&mut self, tx_id: u64, timeout: Option<Duration>) -> Result<(), CqlError> {
        if self.open.is_some() {
            return Err(CqlError::Invalid(
                "BEGIN received while a transaction is already open (nested transactions \
                 are not supported)"
                    .to_string(),
            ));
        }
        self.open = Some(tx_id);
        self.staged.clear();
        self.deadline = timeout.map(|budget| (Instant::now() + budget, budget));
        Ok(())
    }

    /// If the open transaction has a deadline that has passed, ABORT it (discard
    /// staged writes, close the tx) and return the timeout error. Otherwise
    /// `Ok(())`. This is the single enforcement point shared by `stage` and
    /// `commit` so a timed-out transaction can never make progress.
    fn check_deadline(&mut self) -> Result<(), CqlError> {
        if let Some((deadline, budget)) = self.deadline {
            let now = Instant::now();
            if now >= deadline {
                let elapsed = now.duration_since(deadline) + budget;
                self.abort_state();
                return Err(CqlError::TransactionTimeout {
                    timeout_ms: budget.as_millis() as u64,
                    elapsed_ms: elapsed.as_millis() as u64,
                });
            }
        }
        Ok(())
    }

    /// Reset the machine to the no-open-transaction state, discarding any staged
    /// writes. Used by abort/commit/rollback — nothing staged is ever persisted
    /// by this call.
    fn abort_state(&mut self) {
        self.staged.clear();
        self.open = None;
        self.deadline = None;
    }

    /// Stage a write onto the open transaction's batch. FAILS LOUD if no
    /// transaction is open (a `RUN` write must not silently escape the tx) or if
    /// the transaction's timeout has already elapsed (URS-QEC-B03) — in which
    /// case the transaction is aborted and nothing is staged.
    pub fn stage(&mut self, op: BatchOp) -> Result<(), CqlError> {
        if self.open.is_none() {
            return Err(CqlError::Invalid(
                "write staged with no open explicit transaction".to_string(),
            ));
        }
        self.check_deadline()?;
        self.staged.push(op);
        Ok(())
    }

    /// The connection's own staged writes (read-your-own-writes inside the tx).
    pub fn staged_ops(&self) -> &[BatchOp] {
        &self.staged
    }

    /// Atomically commit the staged batch via the storage primitive.
    ///
    /// Opens a `BatchTxn` from `engine.begin_batch()`, stages every queued op,
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
        // Enforce the per-tx timeout BEFORE persisting: a transaction whose
        // budget has elapsed is aborted and FAILS LOUD — nothing is committed
        // (URS-QEC-B03, fail-loud, never fake).
        self.check_deadline()?;
        // Take ownership of the staged ops and close the tx regardless of
        // outcome — a commit attempt ends the transaction either way.
        let ops = std::mem::take(&mut self.staged);
        self.open = None;
        self.deadline = None;

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
        self.abort_state();
        Ok(())
    }
}

/// Per-connection buffer for a **cluster-wide** CQL `BEGIN`/`COMMIT` transaction.
///
/// Distinct from [`ConnTxn`] (which commits a *local* atomic batch on the single
/// engine for Bolt): this buffers DML between `BEGIN` and `COMMIT` and, on
/// `COMMIT`, commits the WHOLE write-set as one multi-key Accord transaction via
/// the injected [`TransactionCommitter`] (ADR-021). `ROLLBACK` drops the buffer.
///
/// FAIL-LOUD (URS-QEC-X01): a failed commit returns `Err` and the transaction is
/// closed — the server never acks a transaction it did not commit.
#[derive(Default)]
pub struct CqlTransaction {
    open: bool,
    buffer: Vec<TransactionWrite>,
}

impl CqlTransaction {
    /// A fresh connection with no open transaction.
    pub fn new() -> Self {
        Self::default()
    }

    /// `true` while a transaction is open.
    pub fn is_open(&self) -> bool {
        self.open
    }

    /// Number of DML writes buffered so far.
    pub fn staged_len(&self) -> usize {
        self.buffer.len()
    }

    /// `BEGIN`: open a transaction. FAILS LOUD on a nested `BEGIN`.
    pub fn begin(&mut self) -> Result<(), CqlError> {
        if self.open {
            return Err(CqlError::Invalid(
                "nested transactions are not supported".to_string(),
            ));
        }
        self.open = true;
        self.buffer.clear();
        Ok(())
    }

    /// Buffer one DML write. FAILS LOUD if no transaction is open.
    pub fn stage(&mut self, write: TransactionWrite) -> Result<(), CqlError> {
        if !self.open {
            return Err(CqlError::Invalid(
                "DML staged outside of a transaction".to_string(),
            ));
        }
        self.buffer.push(write);
        Ok(())
    }

    /// `COMMIT`: commit the buffered write-set via `committer`, then close the
    /// transaction. FAILS LOUD if no transaction is open. The buffer is drained
    /// and the transaction closed regardless of outcome — a failed commit is
    /// surfaced as `Err` (never silently retried or acked), a clean abort as the
    /// returned [`CommitOutcome`].
    pub async fn commit(
        &mut self,
        committer: &dyn TransactionCommitter,
    ) -> Result<CommitOutcome, CqlError> {
        if !self.open {
            return Err(CqlError::Invalid(
                "COMMIT outside of a transaction".to_string(),
            ));
        }
        let writes = std::mem::take(&mut self.buffer);
        self.open = false;
        committer
            .commit(writes)
            .await
            .map_err(|e| CqlError::ServerError(format!("transaction commit failed: {}", e.reason)))
    }

    /// `ROLLBACK`: discard the buffer and close. FAILS LOUD if not open.
    pub fn rollback(&mut self) -> Result<(), CqlError> {
        if !self.open {
            return Err(CqlError::Invalid(
                "ROLLBACK outside of a transaction".to_string(),
            ));
        }
        self.buffer.clear();
        self.open = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    fn tw(ks: &str, key: &[u8]) -> TransactionWrite {
        TransactionWrite {
            keyspace: ks.to_string(),
            key: key.to_vec(),
            mutation: b"m".to_vec(),
        }
    }

    #[test]
    fn cql_txn_begin_stage_rollback() {
        let mut tx = CqlTransaction::new();
        assert!(!tx.is_open());
        tx.begin().unwrap();
        assert!(tx.is_open());
        assert!(tx.begin().is_err(), "nested BEGIN must fail loud");
        tx.stage(tw("ks", b"a")).unwrap();
        tx.stage(tw("ks", b"b")).unwrap();
        assert_eq!(tx.staged_len(), 2);
        tx.rollback().unwrap();
        assert!(!tx.is_open());
        assert_eq!(tx.staged_len(), 0, "ROLLBACK drops the buffer");
    }

    #[test]
    fn cql_txn_stage_and_rollback_outside_fail() {
        let mut tx = CqlTransaction::new();
        assert!(tx.stage(tw("ks", b"a")).is_err());
        assert!(tx.rollback().is_err());
    }

    #[tokio::test]
    async fn cql_txn_commit_sends_buffer_to_committer_and_closes() {
        use ferrosa_storage::accord::MockTransactionCommitter;
        let committer = MockTransactionCommitter::new();
        let mut tx = CqlTransaction::new();
        tx.begin().unwrap();
        tx.stage(tw("ks", b"a")).unwrap();
        tx.stage(tw("ks", b"b")).unwrap();

        let outcome = tx.commit(&committer).await.unwrap();

        assert_eq!(outcome, CommitOutcome::Committed);
        assert!(!tx.is_open(), "COMMIT closes the transaction");
        assert_eq!(tx.staged_len(), 0, "buffer is drained on commit");
        assert_eq!(
            committer.committed(),
            vec![vec![tw("ks", b"a"), tw("ks", b"b")]],
            "the exact buffered write-set reaches the committer, in order"
        );
    }

    #[tokio::test]
    async fn cql_txn_commit_outside_fails() {
        use ferrosa_storage::accord::MockTransactionCommitter;
        let committer = MockTransactionCommitter::new();
        let mut tx = CqlTransaction::new();
        assert!(tx.commit(&committer).await.is_err());
    }

    #[tokio::test]
    async fn cql_txn_commit_failure_surfaced_and_closes() {
        use ferrosa_storage::accord::{CommitError, MockTransactionCommitter};
        let committer = MockTransactionCommitter::with_result(Err(CommitError {
            reason: "quorum unavailable".to_string(),
        }));
        let mut tx = CqlTransaction::new();
        tx.begin().unwrap();
        tx.stage(tw("ks", b"a")).unwrap();

        let result = tx.commit(&committer).await;

        assert!(
            result.is_err(),
            "a failed commit must surface as Err — never ack an uncommitted transaction"
        );
        assert!(
            !tx.is_open(),
            "the transaction closes even when commit fails"
        );
    }

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
