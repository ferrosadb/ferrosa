//! The open transaction's write-set, staged so it never accumulates in memory.
//!
//! A PostgreSQL transaction buffers its DML until `COMMIT`: an `INSERT`/`UPDATE`/
//! `DELETE` inside a `T` block, and every row of a `COPY ... FROM STDIN`, is held
//! for the single atomic apply at commit. Holding that write-set as a plain
//! `Vec<PgWrite>` makes front-end memory grow with the row count — a bulk load
//! under `BEGIN; COPY ...; COMMIT` OOMs the node (FMEA PG-12).
//!
//! [`TxnWriteSet`] closes that: each row is serialized to one
//! [`Mutation`](ferrosa_storage::Mutation) frame and handed to a
//! [`WriteSetStage`], which keeps a bounded, tunable resident prefix and SPILLS
//! the rest to a private staging file. Front-end residency is therefore the
//! staging buffer (default 8 MiB, `FERROSA_WRITE_SET_SPILL_THRESHOLD_BYTES`),
//! never the load.
//!
//! There is deliberately NO capacity refusal here. A transaction larger than the
//! buffer spills; it is never rejected for being large, so the old
//! `FERROSA_POSTGRES_MAX_TXN_WRITES` (SQLSTATE `53400`) refusal is gone.
//!
//! # The read path is a peek, the commit path is a stream
//!
//! - [`TxnWriteSet::pending_writes`] decodes the staged set back into a resident
//!   `Vec<PgWrite>` for READ-YOUR-OWN-WRITES — a `SELECT` or scalar subquery
//!   inside the transaction must see its uncommitted rows. This is the only
//!   place the set is materialized, and only a query that actually reads pays it.
//! - [`TxnWriteSet::into_staged`] hands the staging to the commit path, which
//!   reads it as a repeatable STREAM (see `StorageEngine::write_atomic_batch`), so
//!   the COMMIT peak does not re-materialize the set either.

use std::path::PathBuf;

use ferrosa_common::{Error, Result};
use ferrosa_storage::write_set_stage::{StagedWriteSet, WriteSetSource, WriteSetStage};
use ferrosa_storage::Mutation;

use crate::mvcc::writer_ownership::{stripe, WriterLease, STRIPES};
use crate::mvcc::PgWrite;

/// The default staging root for a transaction write-set. Matches
/// `WriteSetSpill`'s convention: a private per-transaction directory under the
/// process temp dir, created lazily on the first spill.
fn default_stage_root() -> PathBuf {
    std::env::temp_dir().join("ferrosa-pg-txn-stage")
}

/// A transaction's buffered write-set, backed by a threshold-bounded, spilling
/// [`WriteSetStage`] rather than a resident `Vec<PgWrite>`.
pub struct TxnWriteSet {
    pub(crate) read_committed: bool,
    pub(crate) writers: WriterLease,
    unowned_tables: std::collections::BTreeSet<(String, uuid::Uuid)>,
    stage: WriteSetStage,
    /// Number of writes staged. Tracked apart from the stage so `len` is O(1)
    /// and independent of residency.
    count: usize,
    /// The qualified `schema.table` of every table staged into this write-set.
    /// A per-TABLE set, so it is bounded by the schema's table count, never by the
    /// row count — it stays resident while the rows spill. `COMMIT` hands it to the
    /// distributed commit so each written table contributes its WRITE WATERMARK,
    /// which a transaction that SCANNED the table reads (FMEA CL-72).
    tables: std::collections::BTreeSet<String>,
    incarnations: std::collections::HashMap<(String, String), uuid::Uuid>,
}

impl std::fmt::Debug for TxnWriteSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnWriteSet")
            .field("writes", &self.count)
            .field("resident_bytes", &self.stage.resident_bytes())
            .field("spilled", &self.stage.spilled_to_disk())
            .finish()
    }
}

impl Default for TxnWriteSet {
    fn default() -> Self {
        Self::new(default_stage_root())
    }
}

impl TxnWriteSet {
    /// An empty write-set staging beneath `root`.
    #[must_use]
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            read_committed: false,
            writers: WriterLease::default(),
            unowned_tables: Default::default(),
            stage: WriteSetStage::new(root),
            count: 0,
            tables: std::collections::BTreeSet::new(),
            incarnations: std::collections::HashMap::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_threshold(root: impl Into<PathBuf>, bytes: u64) -> Self {
        Self {
            read_committed: false,
            writers: WriterLease::default(),
            unowned_tables: Default::default(),
            stage: WriteSetStage::with_threshold(root, bytes),
            count: 0,
            tables: Default::default(),
            incarnations: Default::default(),
        }
    }

    /// Stage one buffered write. The mutation is serialized to a frame and handed
    /// to the stage; past the buffer the frame goes to disk, so residency never
    /// tracks [`Self::len`].
    ///
    /// # Errors
    ///
    /// A staging I/O failure is surfaced, never swallowed — a write-set that
    /// silently dropped a row would commit a transaction missing writes.
    pub(crate) fn push(&mut self, write: PgWrite) -> Result<()> {
        let mutation = write.0;
        if self.read_committed
            && !self
                .writers
                .owns(write.1, stripe(mutation.key.key.as_bytes()))
        {
            self.unowned_tables
                .insert((format!("{}.{}", mutation.keyspace, mutation.table), write.1));
        }
        let name = (mutation.keyspace.clone(), mutation.table.clone());
        if self
            .incarnations
            .get(&name)
            .is_some_and(|id| *id != write.1)
        {
            return Err(Error::InvalidData(
                "transaction spans different table incarnations".into(),
            ));
        }
        self.incarnations.insert(name, write.1);
        self.tables
            .insert(format!("{}.{}", mutation.keyspace, mutation.table));
        let mut frame = vec![0u8; mutation.serialized_size()];
        mutation.serialize_into(&mut frame);
        self.stage.append(&frame)?;
        self.count += 1;
        Ok(())
    }

    /// The qualified `schema.table` of every table written, in sorted order. One
    /// entry per TABLE, so this is bounded by the schema, not the row count.
    #[must_use]
    pub(crate) fn written_tables(&self) -> Vec<String> {
        self.tables.iter().cloned().collect()
    }

    /// Number of writes staged so far.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.count
    }

    /// Whether nothing has been staged.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Payload bytes still resident. Bounded by the staging buffer, independent
    /// of [`Self::len`].
    #[cfg(test)]
    #[must_use]
    pub(crate) fn resident_bytes(&self) -> u64 {
        self.stage.resident_bytes()
    }

    /// The resident staging buffer this write-set was configured with.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn threshold_bytes(&self) -> u64 {
        self.stage.threshold_bytes()
    }

    /// Whether the write-set has spilled to disk.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn spilled_to_disk(&self) -> bool {
        self.stage.spilled_to_disk()
    }

    /// Decode every staged write back into a resident `Vec<PgWrite>`.
    ///
    /// This is the READ-YOUR-OWN-WRITES path only: a `SELECT` inside the open
    /// transaction must see the rows the transaction has already written. The
    /// commit path does NOT use this — it consumes the staging as a stream — so a
    /// transaction that only writes never pays the materialization.
    ///
    /// # Errors
    ///
    /// A staging read or frame decode failure is surfaced, never swallowed.
    pub(crate) fn pending_writes(&mut self) -> Result<Vec<PgWrite>> {
        let mut out: Vec<PgWrite> = Vec::with_capacity(self.count);
        self.stage.for_each_staged(&mut |frame| {
            let mutation = Mutation::deserialize_from(frame).map_err(|e| {
                Error::InvalidData(format!(
                    "txn write-set: staged entry is not a decodable mutation frame: {e}"
                ))
            })?;
            let id = self.incarnations[&(mutation.keyspace.clone(), mutation.table.clone())];
            out.push(PgWrite(mutation, id));
            Ok(())
        })?;
        Ok(out)
    }

    /// Consume the write-set for `COMMIT`, as a repeatable streaming source.
    ///
    /// # Errors
    ///
    /// A staging flush or map failure is surfaced.
    pub(crate) fn into_staged(self) -> Result<PgStagedWriteSet> {
        Ok(PgStagedWriteSet {
            writers: self.writers,
            unowned_tables: self.unowned_tables,
            inner: self.stage.finish()?,
            incarnations: self.incarnations,
        })
    }
}

/// A streamed write source retains relation identities independently of spilled rows.
pub(crate) struct PgStagedWriteSet {
    pub(crate) writers: WriterLease,
    unowned_tables: std::collections::BTreeSet<(String, uuid::Uuid)>,
    inner: StagedWriteSet,
    incarnations: std::collections::HashMap<(String, String), uuid::Uuid>,
}
impl PgStagedWriteSet {
    /// Visit spilled mutations without retaining per-row ownership metadata.
    pub(crate) async fn acquire_writers(
        &mut self,
        mvcc: &crate::mvcc::MvccManager,
        original: &crate::mvcc::MvccSnapshot,
    ) -> std::result::Result<(), crate::messages::BackendMessage> {
        let mut wanted = std::collections::BTreeMap::<uuid::Uuid, [bool; STRIPES]>::new();
        self.inner
            .for_each_mutation(&mut |mutation| {
                let id = self.incarnations[&(mutation.keyspace.clone(), mutation.table.clone())];
                let slots = wanted.entry(id).or_insert([false; STRIPES]);
                if ferrosa_storage::table_tombstone::is_table_tombstone_key(&mutation.key) {
                    slots.fill(true);
                } else {
                    let slot = stripe(mutation.key.key.as_bytes());
                    slots[slot] = true;
                }
                Ok(())
            })
            .map_err(|e| crate::query::write_error_response(&e))?;
        for (id, slots) in wanted {
            for (slot, needed) in slots.into_iter().enumerate() {
                if needed {
                    self.writers.acquire(&mvcc.writers, id, slot).await?;
                }
            }
        }
        // Synchronous bulk staging can reach COMMIT without early ownership.
        // It must retain conflict validation after the wait, rather than
        // publishing an old-timestamp mutation over a newer committed row.
        if self
            .unowned_tables
            .iter()
            .any(|(table, id)| mvcc.relation_changed_since(original, table, *id))
        {
            return Err(crate::query::error_response(
                "40001",
                "concurrent change preceded buffered writer ownership",
            ));
        }
        Ok(())
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}
impl WriteSetSource for PgStagedWriteSet {
    fn mutation_count(&self) -> usize {
        self.inner.mutation_count()
    }
    fn for_each_mutation(&self, visit: &mut dyn FnMut(&Mutation) -> Result<()>) -> Result<()> {
        self.inner.for_each_mutation(visit)
    }
    fn for_each_owned_mutation(&self, visit: &mut dyn FnMut(Mutation) -> Result<()>) -> Result<()> {
        self.inner.for_each_owned_mutation(visit)
    }
}

pub(crate) trait PgWriteSource: WriteSetSource {
    /// Called with the schema lifecycle read lease held, before any batch effects.
    fn validate_incarnations(
        &self,
        _schema: &ferrosa_schema::Schema,
    ) -> std::result::Result<(), crate::mvcc::MvccCommitError> {
        Ok(())
    }
}
// Resident autocommit mutations are validated against the statement's bound UUID
// before entering commit_mutations, under the same lifecycle lease.
impl PgWriteSource for Vec<Mutation> {}
#[cfg(test)]
impl PgWriteSource for StagedWriteSet {}
impl PgWriteSource for PgStagedWriteSet {
    fn validate_incarnations(
        &self,
        schema: &ferrosa_schema::Schema,
    ) -> std::result::Result<(), crate::mvcc::MvccCommitError> {
        let snap = schema.snapshot();
        if self
            .incarnations
            .iter()
            .any(|(name, id)| snap.tables.get(name).is_none_or(|table| table.id != *id))
        {
            return Err(crate::mvcc::MvccCommitError::SerializationFailure);
        }
        Ok(())
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use crate::mvcc::{MvccManager, RowChange};

    #[tokio::test]
    async fn late_staged_write_cannot_gain_freshness_from_later_stripe_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let mvcc = MvccManager::default();
        let original = mvcc.snapshot();
        let id = uuid::Uuid::new_v4();
        let key =
            ferrosa_common::DecoratedKey::new(ferrosa_common::PartitionKey::new(b"key".to_vec()));
        let mut writes = TxnWriteSet::new(dir.path());
        writes.read_committed = true;
        // COPY uses this synchronous staging seam, before acquiring writer ownership.
        writes
            .push(PgWrite(
                Mutation::new("ks".into(), "t".into(), key.clone(), vec![], 1),
                id,
            ))
            .unwrap();
        mvcc.commit(
            &mvcc.snapshot(),
            &Default::default(),
            vec![RowChange {
                table: "ks.t".into(),
                table_id: id,
                key: vec![ferrosa_sql::Value::Text("key".into())],
                partition_key: b"key".to_vec(),
                before: None,
                after: Some(ferrosa_sql::Row::new(vec![ferrosa_sql::Value::Int(2)])),
            }],
            || Ok(()),
        )
        .unwrap();
        // A later DML can acquire the same stripe; that must not bless the earlier frame.
        writes
            .writers
            .acquire(&mvcc.writers, id, stripe(key.key.as_bytes()))
            .await
            .unwrap();
        let mut staged = writes.into_staged().unwrap();
        let error = staged.acquire_writers(&mvcc, &original).await.unwrap_err();
        assert!(
            matches!(error, crate::messages::BackendMessage::ErrorResponse { fields }
            if fields.iter().any(|(kind, value)| *kind == b'C' && value == "40001"))
        );
    }
}
