//! Transaction commit seam (ADR-021) — the front-end-facing boundary for
//! multi-key SQL transactions.
//!
//! CQL and Postgres `BEGIN`/`COMMIT` buffer their DML into a write-set and call
//! [`TransactionCommitter::commit`] on `COMMIT`; `ROLLBACK` drops the buffer
//! without calling it. The `ferrosa-cluster` implementation resolves each key's
//! replicas (this ADR's `WritePath`) and drives a multi-key Accord transaction
//! (per-shard quorum, atomic apply). The trait lives here, in the crate both
//! front-ends already share, so neither front-end has to depend on the cluster
//! layer — mirroring how `StorageApplier` is injected.

use async_trait::async_trait;

/// One buffered write in a transaction: the partition key, its encoded
/// commit-log mutation, and the keyspace whose replication settings determine
/// the key's replica placement (resolved by the cluster implementation).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionWrite {
    /// Keyspace the write targets — selects the replication strategy.
    pub keyspace: String,
    /// Raw partition-key bytes (Accord conflict-ordering + routing key).
    pub key: Vec<u8>,
    /// Encoded self-describing commit-log mutation to apply for this key.
    pub mutation: Vec<u8>,
}

/// Outcome of committing a buffered transaction write-set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    /// Every write in the set committed atomically.
    Committed,
    /// The transaction aborted cleanly; no write was applied.
    Aborted { reason: String },
}

/// The commit path could not reach a decision (e.g. quorum unavailable). This is
/// distinct from a clean [`CommitOutcome::Aborted`]: the caller must surface it
/// and MUST NOT report success — never ack a transaction we did not commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitError {
    pub reason: String,
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "transaction commit failed: {}", self.reason)
    }
}

impl std::error::Error for CommitError {}

/// Front-end-facing seam for committing a multi-key SQL transaction.
///
/// Implementations MUST be fail-loud: return `Err(CommitError)` (or
/// `Ok(Aborted)`) rather than reporting success for a transaction that did not
/// durably commit. An empty write-set (`BEGIN; COMMIT;` with no DML) is a no-op
/// that commits cleanly.
#[async_trait]
pub trait TransactionCommitter: Send + Sync {
    /// Commit `writes` as one atomic multi-key transaction.
    async fn commit(&self, writes: Vec<TransactionWrite>) -> Result<CommitOutcome, CommitError>;
}

/// In-memory [`TransactionCommitter`] for tests: records every committed
/// write-set and returns a configurable outcome, so a front-end can unit-test
/// `BEGIN`/`COMMIT`/`ROLLBACK` buffering without a live cluster.
pub struct MockTransactionCommitter {
    committed: std::sync::Mutex<Vec<Vec<TransactionWrite>>>,
    result: Result<CommitOutcome, CommitError>,
}

impl MockTransactionCommitter {
    /// A committer that always reports `Committed`.
    pub fn new() -> Self {
        Self {
            committed: std::sync::Mutex::new(Vec::new()),
            result: Ok(CommitOutcome::Committed),
        }
    }

    /// A committer that always returns `result` (an outcome or a `CommitError`).
    pub fn with_result(result: Result<CommitOutcome, CommitError>) -> Self {
        Self {
            committed: std::sync::Mutex::new(Vec::new()),
            result,
        }
    }

    /// Every write-set passed to [`commit`](TransactionCommitter::commit), in
    /// call order.
    pub fn committed(&self) -> Vec<Vec<TransactionWrite>> {
        self.committed.lock().expect("committer mutex").clone()
    }
}

impl Default for MockTransactionCommitter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionCommitter for MockTransactionCommitter {
    async fn commit(&self, writes: Vec<TransactionWrite>) -> Result<CommitOutcome, CommitError> {
        self.committed.lock().expect("committer mutex").push(writes);
        self.result.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(ks: &str, key: &[u8]) -> TransactionWrite {
        TransactionWrite {
            keyspace: ks.to_string(),
            key: key.to_vec(),
            mutation: b"row".to_vec(),
        }
    }

    #[tokio::test]
    async fn mock_records_committed_write_set() {
        let committer = MockTransactionCommitter::new();
        let writes = vec![write("ks", b"a"), write("ks", b"b")];

        let outcome = committer.commit(writes.clone()).await.expect("commit");

        assert_eq!(outcome, CommitOutcome::Committed);
        assert_eq!(
            committer.committed(),
            vec![writes],
            "the committer must record the exact write-set it was handed"
        );
    }

    #[tokio::test]
    async fn empty_write_set_commits_cleanly() {
        // BEGIN; COMMIT; with no DML is a no-op that commits.
        let committer = MockTransactionCommitter::new();
        let outcome = committer.commit(Vec::new()).await.expect("commit");
        assert_eq!(outcome, CommitOutcome::Committed);
    }

    #[tokio::test]
    async fn configured_abort_outcome_is_surfaced() {
        let committer = MockTransactionCommitter::with_result(Ok(CommitOutcome::Aborted {
            reason: "condition not met".to_string(),
        }));
        let outcome = committer.commit(vec![write("ks", b"a")]).await.expect("ok");
        assert_eq!(
            outcome,
            CommitOutcome::Aborted {
                reason: "condition not met".to_string()
            }
        );
    }

    #[tokio::test]
    async fn configured_commit_error_is_surfaced() {
        // A commit that cannot reach a decision must propagate as Err — the
        // front-end must never report success for it.
        let committer = MockTransactionCommitter::with_result(Err(CommitError {
            reason: "quorum unavailable".to_string(),
        }));
        let err = committer
            .commit(vec![write("ks", b"a")])
            .await
            .expect_err("must surface the commit error");
        assert_eq!(err.reason, "quorum unavailable");
    }
}
