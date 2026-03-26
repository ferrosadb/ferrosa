//! Session-level state for CQL connections.
//!
//! Tracks transaction state (Accord transactions) and validates statement
//! transitions. Nested transactions are rejected. DDL inside transactions
//! is rejected.

use crate::ast::Statement;
use crate::error::CqlError;

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
