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

use super::postgres_mvcc::PostgresMvccApplyObserver;
use async_trait::async_trait;
use ferrosa_common::accord::Timestamp;
use std::sync::Arc;

use crate::write_set_spill::{WriteSetSpill, WriteSetSpillBuilder};

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

/// A transaction's Accord write-set, **staged** so its payload bulk never has to be
/// resident on the coordinator.
///
/// The former shape built a `Vec<TransactionWrite>` — one encoded mutation per key,
/// every payload in RAM — and handed it to the committer, which copied it again into a
/// spill. For `pgbench -i --scale 10` (1.1M rows in one transaction) that Vec *is* the
/// COMMIT's memory problem. This type replaces it: the payloads live in a
/// [`WriteSetSpill`] (memory-mapped temp file, indexed by key position), and only the
/// per-key ROUTING metadata — `(keyspace, partition key)`, one small entry per partition,
/// which Accord's conflict ordering and per-shard participant set genuinely need — stays
/// resident. It is produced by [`StagedAccordWriteSetBuilder`], which a streaming producer
/// drives one `TransactionWrite` at a time.
///
/// # Invariants
///
/// - **Complete**: every accepted write is either in [`Self::keys`] (with its payload readable
///   at the same index through [`Self::payload_ref`]) or in [`Self::tombstones`]. Nothing is
///   dropped.
/// - **Ordered**: [`Self::keys`] is in the producer's write order; entry `i` reads the
///   payload accepted for `keys[i]`.
/// - **Fail loud**: reading a payload index that was never staged is an error, never an
///   empty mutation the applier silently skips.
pub struct StagedAccordWriteSet {
    /// `(keyspace, partition key)` per ordinary write, in write order. The small resident
    /// routing index; `keys[i]`'s payload is `payloads.entry(i)`.
    keys: Vec<(String, Vec<u8>)>,
    /// The encoded mutation payloads, in write order, indexed by key position. Disk-backed
    /// past the staging buffer.
    payloads: Arc<WriteSetSpill>,
    /// Table-tombstone (`TRUNCATE`) entries, held apart from `keys`/`payloads` because they
    /// must be replicated to EVERY serving node at CL=ALL, never routed through per-key
    /// Accord (which would reach only the reserved key's RF replica set and resurrect the
    /// rows elsewhere). One entry per TRUNCATE, so this is tiny regardless of row count.
    tombstones: Vec<TransactionWrite>,
}

impl std::fmt::Debug for StagedAccordWriteSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedAccordWriteSet")
            .field("keys", &self.keys.len())
            .field("payload_bytes", &self.payloads.bytes())
            .field("resident_payload_bytes", &self.payloads.resident_bytes())
            .field("tombstones", &self.tombstones.len())
            .finish()
    }
}

impl StagedAccordWriteSet {
    /// Number of ORDINARY (non-tombstone) writes in the driven write-set.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether there are no ordinary writes. A tombstone-only transaction is still
    /// non-empty as a commit, so callers that must not skip the tombstone path test
    /// [`Self::tombstones`] separately.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// The `(keyspace, partition key)` routing index, in write order.
    #[must_use]
    pub fn keys(&self) -> &[(String, Vec<u8>)] {
        &self.keys
    }

    /// The table-tombstone entries to replicate to every serving node.
    #[must_use]
    pub fn tombstones(&self) -> &[TransactionWrite] {
        &self.tombstones
    }

    /// Borrow the encoded payload for key `index` — a slice of the spill's memory map, not
    /// a fresh allocation.
    ///
    /// # Errors
    ///
    /// An index that was never staged — never a silent empty read.
    pub fn payload_ref(&self, index: usize) -> ferrosa_common::Result<&[u8]> {
        self.payloads.entry(index)
    }

    /// The encoded payload for key `index` as an owned copy (the twin of
    /// [`Self::payload_ref`] for a consumer that must own the bytes).
    ///
    /// # Errors
    ///
    /// An index that was never staged.
    pub fn payload(&self, index: usize) -> ferrosa_common::Result<Vec<u8>> {
        self.payloads.mutation(index)
    }

    /// The spill holding this set's payloads, for a consumer that keeps it alive across a
    /// `'static` dispatch (the cluster driver's apply path owns the `Arc` and borrows the
    /// mapping inside it).
    #[must_use]
    pub fn payloads_arc(&self) -> Arc<WriteSetSpill> {
        Arc::clone(&self.payloads)
    }

    /// Materialize the whole set into a resident `Vec<TransactionWrite>`. TEST ONLY: this
    /// is the shape the staged type exists to remove; production never calls it.
    #[doc(hidden)]
    pub fn to_resident(&self) -> ferrosa_common::Result<Vec<TransactionWrite>> {
        let mut out = Vec::with_capacity(self.keys.len() + self.tombstones.len());
        for (index, (keyspace, key)) in self.keys.iter().enumerate() {
            out.push(TransactionWrite {
                keyspace: keyspace.clone(),
                key: key.clone(),
                mutation: self.payload(index)?,
            });
        }
        out.extend(self.tombstones.iter().cloned());
        Ok(out)
    }
}

/// Build a [`StagedAccordWriteSet`] one `TransactionWrite` at a time, so the producer never
/// has to hold the whole write-set.
///
/// [`append`](Self::accept) hands each encoded write to a bounded, tunable resident buffer
/// ([`crate::write_set_stage::WRITE_SET_SPILL_THRESHOLD_ENV`]) that spills past its limit;
/// table tombstones are carried separately (they never enter the payload spill). There is
/// deliberately NO capacity refusal: a write-set larger than the buffer spills, it is
/// never rejected.
pub struct StagedAccordWriteSetBuilder {
    keys: Vec<(String, Vec<u8>)>,
    payloads: WriteSetSpillBuilder,
    tombstones: Vec<TransactionWrite>,
    /// The reserved table-tombstone partition key bytes, resolved once so each `accept`
    /// compares against it without rebuilding the key.
    tombstone_key: Vec<u8>,
}

impl std::fmt::Debug for StagedAccordWriteSetBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedAccordWriteSetBuilder")
            .field("keys", &self.keys.len())
            .field("resident_payload_bytes", &self.payloads.resident_bytes())
            .field("spilled_to_disk", &self.payloads.spilled_to_disk())
            .field("tombstones", &self.tombstones.len())
            .finish()
    }
}

impl Default for StagedAccordWriteSetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StagedAccordWriteSetBuilder {
    /// A builder using the process-wide staging-buffer setting.
    #[must_use]
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            payloads: WriteSetSpillBuilder::new(),
            tombstones: Vec::new(),
            tombstone_key: crate::table_tombstone::table_tombstone_key()
                .key
                .as_bytes()
                .to_vec(),
        }
    }

    /// A builder keeping `threshold_bytes` of payload resident before spilling.
    #[must_use]
    pub fn with_threshold_bytes(threshold_bytes: u64) -> Self {
        Self {
            keys: Vec::new(),
            payloads: WriteSetSpillBuilder::with_threshold(threshold_bytes),
            tombstones: Vec::new(),
            tombstone_key: crate::table_tombstone::table_tombstone_key()
                .key
                .as_bytes()
                .to_vec(),
        }
    }

    /// Number of writes accepted so far (ordinary + tombstone).
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len() + self.tombstones.len()
    }

    /// Whether nothing has been accepted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Accept one write. A table tombstone is carried apart; every other write's payload is
    /// streamed into the spill and its routing key appended, so `keys[i]` and the payload at
    /// index `i` are appended in the same call and stay aligned.
    ///
    /// # Errors
    ///
    /// A staging I/O failure — surfaced, never swallowed (a dropped payload would commit a
    /// transaction missing rows).
    pub fn accept(&mut self, write: TransactionWrite) -> ferrosa_common::Result<()> {
        if write.key == self.tombstone_key {
            self.tombstones.push(write);
            return Ok(());
        }
        self.payloads.append(&write.mutation)?;
        self.keys.push((write.keyspace, write.key));
        Ok(())
    }

    /// Finish the staging, memory-mapping any spilled region.
    ///
    /// # Errors
    ///
    /// A flush, reopen, or mmap failure.
    pub fn finish(self) -> ferrosa_common::Result<StagedAccordWriteSet> {
        Ok(StagedAccordWriteSet {
            keys: self.keys,
            payloads: Arc::new(self.payloads.finish()?),
            tombstones: self.tombstones,
        })
    }
}

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

    /// Register the PostgreSQL MVCC observer on this node's local Accord apply
    /// path. Cluster implementations delegate to their storage applier.
    fn register_postgres_mvcc_observer(
        &self,
        _observer: std::sync::Arc<dyn PostgresMvccApplyObserver>,
    ) -> Result<(), CommitError> {
        Ok(())
    }

    /// Establish an Accord-ordered snapshot boundary for a PostgreSQL
    /// transaction. This is deliberately separate from `commit`: CQL callers
    /// keep their existing protocol and do not acquire PostgreSQL snapshot
    /// barriers.
    async fn begin_postgres_snapshot(&self, keyspace: &str) -> Result<Timestamp, CommitError> {
        Err(CommitError {
            reason: format!(
                "committer does not implement PostgreSQL snapshot barriers for keyspace '{keyspace}'"
            ),
        })
    }

    /// Check that a PostgreSQL snapshot is still current before exposing rows
    /// from a distributed read. Implementations return `false` when a committed
    /// write is newer than the snapshot. The default fails closed.
    async fn validate_postgres_snapshot(
        &self,
        keyspace: &str,
        _snapshot: Timestamp,
    ) -> Result<bool, CommitError> {
        Err(CommitError {
            reason: format!(
                "committer does not implement PostgreSQL snapshot validation for keyspace '{keyspace}'"
            ),
        })
    }

    /// Commit PostgreSQL writes after checking the captured snapshot against
    /// every table the transaction read or wrote. Implementations must order
    /// this validation with the writes in Accord. The default fails closed.
    ///
    /// `writes` is a [`StagedAccordWriteSet`]: the payload bulk may live on disk behind a
    /// bounded buffer, so the committer must read payloads through it rather than assume a
    /// resident `Vec`.
    async fn commit_postgres(
        &self,
        _keyspace: &str,
        _writes: StagedAccordWriteSet,
        _tables: Vec<String>,
        _snapshot: Timestamp,
    ) -> Result<CommitOutcome, CommitError> {
        Err(CommitError {
            reason: "committer does not implement Accord-ordered PostgreSQL snapshot validation"
                .to_string(),
        })
    }

    /// Commit `writes` and evaluate `reads` at the transaction's agreed commit
    /// timestamp, returning the row bytes observed for each read (positional).
    ///
    /// The default implementation supports only an EMPTY read-set — the shape
    /// used by front-ends that never buffer transactional reads (e.g. the
    /// Postgres front-end) and by write-only CQL transactions. A NON-empty
    /// read-set against a committer that has not overridden this method is a
    /// hard error: silently returning `CommitReads::default()` would fabricate
    /// row-ABSENCE (`None` means "absent at commit-t" to the caller), turning a
    /// missing capability into wrong query results. Fail loud instead; the
    /// front-end surfaces the error and the transaction does not lie.
    async fn commit_with_reads(
        &self,
        writes: Vec<TransactionWrite>,
        reads: Vec<TransactionRead>,
    ) -> Result<(CommitOutcome, CommitReads), CommitError> {
        if !reads.is_empty() {
            return Err(CommitError {
                reason: format!(
                    "committer does not implement read-in-transaction but {} read(s) were staged;                      refusing to fabricate row-absence",
                    reads.len()
                ),
            });
        }
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

    async fn begin_postgres_snapshot(&self, _keyspace: &str) -> Result<Timestamp, CommitError> {
        Ok(Timestamp {
            epoch: 0,
            time: 1,
            seq: 0,
            node: 0,
        })
    }

    async fn validate_postgres_snapshot(
        &self,
        _keyspace: &str,
        _snapshot: Timestamp,
    ) -> Result<bool, CommitError> {
        Ok(true)
    }

    async fn commit_postgres(
        &self,
        _keyspace: &str,
        writes: StagedAccordWriteSet,
        _tables: Vec<String>,
        _snapshot: Timestamp,
    ) -> Result<CommitOutcome, CommitError> {
        // A test double: materialize the staged set so `committed()` can assert on it. This
        // is the one place materialization is intentional — a mock, not the production path.
        let writes = writes.to_resident().map_err(|error| CommitError {
            reason: error.to_string(),
        })?;
        self.commit(writes).await
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
    async fn default_commit_with_reads_accepts_empty_read_set() {
        let committer = WriteOnlyCommitter;
        let (outcome, reads) = committer
            .commit_with_reads(vec![write("ks", b"a")], Vec::new())
            .await
            .expect("empty read-set must commit through the default impl");
        assert_eq!(outcome, CommitOutcome::Committed);
        assert!(reads.rows.is_empty());
    }

    /// A committer without read-in-transaction support must REFUSE a non-empty
    /// read-set. Silently returning `CommitReads::default()` would fabricate
    /// row-absence (`None` means "absent at commit-t" to the caller) — a
    /// missing capability must never become wrong query results.
    #[tokio::test]
    async fn default_commit_with_reads_rejects_staged_reads_loudly() {
        let committer = WriteOnlyCommitter;
        let err = committer
            .commit_with_reads(vec![write("ks", b"a")], vec![read("ks", "t", b"a")])
            .await
            .expect_err("non-empty read-set against the default impl must fail loud");
        assert!(
            err.reason.contains("read-in-transaction"),
            "error must name the missing capability: {}",
            err.reason
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

    fn txn_write(key: &[u8], mutation: &[u8]) -> TransactionWrite {
        TransactionWrite {
            keyspace: "ks".to_string(),
            key: key.to_vec(),
            mutation: mutation.to_vec(),
        }
    }

    /// The staged builder carries table tombstones APART (they must reach every serving node at
    /// CL=ALL, never be routed through per-key Accord), and keeps each ordinary key's routing
    /// entry aligned with its payload. The resident form is the byte-identity oracle, and an
    /// out-of-range payload read fails loud rather than returning an empty mutation.
    #[test]
    fn staged_builder_carries_tombstones_apart_and_aligns_keys_with_payloads() {
        let tombstone_key = crate::table_tombstone::table_tombstone_key()
            .key
            .as_bytes()
            .to_vec();

        let mut builder = StagedAccordWriteSetBuilder::with_threshold_bytes(u64::MAX);
        builder
            .accept(txn_write(b"a", b"row_a"))
            .expect("stage ordinary a");
        builder
            .accept(TransactionWrite {
                keyspace: "ks".to_string(),
                key: tombstone_key.clone(),
                mutation: b"trunc".to_vec(),
            })
            .expect("stage tombstone");
        builder
            .accept(txn_write(b"b", b"row_b"))
            .expect("stage ordinary b");
        let staged = builder.finish().expect("finish staging");

        assert_eq!(
            staged.len(),
            2,
            "two ORDINARY writes are driven through Accord"
        );
        assert_eq!(
            staged.tombstones().len(),
            1,
            "the tombstone is carried apart"
        );
        assert_eq!(staged.tombstones()[0].mutation, b"trunc");
        assert_eq!(
            staged.keys(),
            &[
                ("ks".to_string(), b"a".to_vec()),
                ("ks".to_string(), b"b".to_vec())
            ],
            "keys stay in write order, tombstone excluded"
        );
        assert_eq!(staged.payload_ref(0).unwrap(), b"row_a");
        assert_eq!(staged.payload_ref(1).unwrap(), b"row_b");

        // Byte-identity: the staged read-back equals the resident form.
        let resident = staged.to_resident().expect("materialize for compare");
        assert_eq!(resident.len(), 3, "two ordinary writes + the tombstone");
        assert_eq!(resident[0], txn_write(b"a", b"row_a"));
        assert_eq!(resident[1], txn_write(b"b", b"row_b"));
        assert_eq!(resident[2].mutation, b"trunc");

        // Out-of-range fails loud — never a silent empty mutation the applier would skip.
        let error = staged.payload(2).expect_err("index 2 was never staged");
        assert!(
            error.to_string().contains("out of range"),
            "a missing payload must fail loud: {error}"
        );
    }

    /// No cap: a write-set far larger than the staging buffer is ACCEPTED and spilled, never
    /// refused, and every payload reads back byte-identically. This is the property the
    /// `Vec<TransactionWrite>` return removed; the `Vec` negative control is in
    /// `write_set_spill`'s builder tests.
    #[test]
    fn staged_builder_streams_a_write_set_far_larger_than_its_buffer() {
        let count = 4096usize;
        let payload = vec![0x7Eu8; 512];
        let mut builder = StagedAccordWriteSetBuilder::with_threshold_bytes(1024);
        for i in 0..count {
            builder
                .accept(txn_write(format!("k{i:08}").as_bytes(), &payload))
                .expect("a large write-set is accepted, never refused");
        }
        let staged = builder.finish().expect("finish staging");
        assert_eq!(staged.len(), count, "no key is dropped");
        assert!(
            staged.payloads_arc().resident_bytes() <= 1024,
            "resident payload stays within the tunable buffer"
        );
        assert!(
            staged.payloads_arc().spilled_to_disk(),
            "the payload bulk left memory for disk"
        );
        for index in [0usize, count / 2, count - 1] {
            assert_eq!(
                staged.payload(index).expect("payload present"),
                payload,
                "payload {index} is intact after staging"
            );
        }
    }
}
