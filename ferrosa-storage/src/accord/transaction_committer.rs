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

/// One buffered read in a transaction: the partition key and the keyspace/table
/// it targets. Evaluated at the transaction's agreed commit timestamp so the
/// returned rows are part of the same atomic transaction as the writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionRead {
    /// Keyspace the read targets.
    pub keyspace: String,
    /// Table the read targets.
    pub table: String,
    /// Raw partition-key bytes to read (Accord conflict-ordering + routing key).
    pub key: Vec<u8>,
}

/// Rows observed by a transaction's read-set, evaluated at the commit timestamp.
/// Positional: `rows[i]` is the agreed row bytes for the `i`-th
/// [`TransactionRead`], or `None` when the row was absent at commit-`t`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommitReads {
    pub rows: Vec<Option<Vec<u8>>>,
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

    /// Commit `writes` and evaluate `reads` at the transaction's agreed commit
    /// timestamp, returning the row bytes observed for each read (positional).
    ///
    /// The default implementation commits the writes and returns no rows — used
    /// by front-ends that never buffer transactional reads (e.g. the Postgres
    /// front-end) and by committers that do not yet support read-in-transaction.
    /// The CQL front-end calls this so a `BEGIN; SELECT; …; COMMIT` can return
    /// the SELECT's rows atomically.
    async fn commit_with_reads(
        &self,
        writes: Vec<TransactionWrite>,
        reads: Vec<TransactionRead>,
    ) -> Result<(CommitOutcome, CommitReads), CommitError> {
        let _ = &reads;
        let outcome = self.commit(writes).await?;
        Ok((outcome, CommitReads::default()))
    }
}

/// In-memory [`TransactionCommitter`] for tests: records every committed
/// write-set and returns a configurable outcome, so a front-end can unit-test
/// `BEGIN`/`COMMIT`/`ROLLBACK` buffering without a live cluster.
pub struct MockTransactionCommitter {
    committed: std::sync::Mutex<Vec<Vec<TransactionWrite>>>,
    read: std::sync::Mutex<Vec<Vec<TransactionRead>>>,
    result: Result<CommitOutcome, CommitError>,
    /// Canned rows returned from [`commit_with_reads`](TransactionCommitter::commit_with_reads),
    /// one per read (positional). Empty ⇒ `None` for every read.
    read_rows: Vec<Option<Vec<u8>>>,
}

impl MockTransactionCommitter {
    /// A committer that always reports `Committed`.
    pub fn new() -> Self {
        Self {
            committed: std::sync::Mutex::new(Vec::new()),
            read: std::sync::Mutex::new(Vec::new()),
            result: Ok(CommitOutcome::Committed),
            read_rows: Vec::new(),
        }
    }

    /// A committer that always returns `result` (an outcome or a `CommitError`).
    pub fn with_result(result: Result<CommitOutcome, CommitError>) -> Self {
        Self {
            committed: std::sync::Mutex::new(Vec::new()),
            read: std::sync::Mutex::new(Vec::new()),
            result,
            read_rows: Vec::new(),
        }
    }

    /// A committer that commits cleanly and echoes `read_rows` (positional) back
    /// from [`commit_with_reads`](TransactionCommitter::commit_with_reads).
    pub fn with_read_rows(read_rows: Vec<Option<Vec<u8>>>) -> Self {
        Self {
            committed: std::sync::Mutex::new(Vec::new()),
            read: std::sync::Mutex::new(Vec::new()),
            result: Ok(CommitOutcome::Committed),
            read_rows,
        }
    }

    /// Every write-set passed to [`commit`](TransactionCommitter::commit), in
    /// call order.
    pub fn committed(&self) -> Vec<Vec<TransactionWrite>> {
        self.committed.lock().expect("committer mutex").clone()
    }

    /// Every read-set passed to
    /// [`commit_with_reads`](TransactionCommitter::commit_with_reads), in call
    /// order.
    pub fn reads(&self) -> Vec<Vec<TransactionRead>> {
        self.read.lock().expect("committer mutex").clone()
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

    async fn commit_with_reads(
        &self,
        writes: Vec<TransactionWrite>,
        reads: Vec<TransactionRead>,
    ) -> Result<(CommitOutcome, CommitReads), CommitError> {
        self.read.lock().expect("committer mutex").push(reads);
        let outcome = self.commit(writes).await?;
        Ok((
            outcome,
            CommitReads {
                rows: self.read_rows.clone(),
            },
        ))
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

    fn read(ks: &str, table: &str, key: &[u8]) -> TransactionRead {
        TransactionRead {
            keyspace: ks.to_string(),
            table: table.to_string(),
            key: key.to_vec(),
        }
    }

    /// A committer with no read-in-transaction support keeps the write-only
    /// behaviour via the default `commit_with_reads`: writes commit, no rows.
    struct WriteOnlyCommitter;
    #[async_trait]
    impl TransactionCommitter for WriteOnlyCommitter {
        async fn commit(
            &self,
            _writes: Vec<TransactionWrite>,
        ) -> Result<CommitOutcome, CommitError> {
            Ok(CommitOutcome::Committed)
        }
    }

    #[tokio::test]
    async fn default_commit_with_reads_commits_writes_and_returns_no_rows() {
        let committer = WriteOnlyCommitter;
        let (outcome, reads) = committer
            .commit_with_reads(vec![write("ks", b"a")], vec![read("ks", "t", b"a")])
            .await
            .expect("commit_with_reads");
        assert_eq!(outcome, CommitOutcome::Committed);
        assert!(
            reads.rows.is_empty(),
            "a write-only committer returns no transactional read rows"
        );
    }

    #[tokio::test]
    async fn mock_records_reads_and_echoes_canned_rows() {
        let committer =
            MockTransactionCommitter::with_read_rows(vec![Some(b"row-a".to_vec()), None]);
        let reads = vec![read("ks", "t", b"a"), read("ks", "t", b"b")];

        let (outcome, got) = committer
            .commit_with_reads(vec![write("ks", b"a")], reads.clone())
            .await
            .expect("commit_with_reads");

        assert_eq!(outcome, CommitOutcome::Committed);
        assert_eq!(got.rows, vec![Some(b"row-a".to_vec()), None]);
        assert_eq!(committer.reads(), vec![reads]);
        assert_eq!(committer.committed(), vec![vec![write("ks", b"a")]]);
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
