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
use ferrosa_storage::write_set_stage::{StagedWriteSet, WriteSetStage};
use ferrosa_storage::Mutation;

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
    stage: WriteSetStage,
    /// Number of writes staged. Tracked apart from the stage so `len` is O(1)
    /// and independent of residency.
    count: usize,
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
            stage: WriteSetStage::new(root),
            count: 0,
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
        let mut frame = vec![0u8; mutation.serialized_size()];
        mutation.serialize_into(&mut frame);
        self.stage.append(&frame)?;
        self.count += 1;
        Ok(())
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
            out.push(PgWrite(mutation));
            Ok(())
        })?;
        Ok(out)
    }

    /// Consume the write-set for `COMMIT`, as a repeatable streaming source.
    ///
    /// # Errors
    ///
    /// A staging flush or map failure is surfaced.
    pub(crate) fn into_staged(self) -> Result<StagedWriteSet> {
        self.stage.finish()
    }
}
