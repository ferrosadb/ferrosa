//! The durability journal's `Applied` marker — the seam finding G1 lives on.
//!
//! # Why this is one entry point
//!
//! A replica answers "did I apply transaction T?" with two facts that look the
//! same and are not:
//!
//! * **The rows.** The storage write (`write_atomic_batch`) fsyncs the engine's
//!   commit log *before* it returns. When the applier returns `Ok`, the row is
//!   durable and readers can already see it. That is the durability instant for
//!   the DATA.
//! * **The protocol-log `Applied` marker.** Written afterwards, it is the
//!   journal's *record* of the decision, used to rebuild per-transaction state
//!   on restart. Crash replay re-drives the apply idempotently on `(txn_id, t)`,
//!   so a missing marker is a gap in the record, never a lost row.
//!
//! Conflating them produced finding G1: a failed marker fsync left the
//! transaction `Committed`, the replica withheld `ApplyOK`, and the coordinator —
//! short of F+1 acknowledgements — reported `TxnAbandoned`, telling the client
//! "NOT committed; safe to retry" while this replica held a durable, readable row.
//!
//! [`record_applied_marker`] is the single function that writes the marker and
//! answers the ack question, with `rows_durable` keeping the two facts apart. The
//! apply path advances the transaction to `Applied` exactly when it returns
//! [`AppliedAck::Ack`], and `Applied` is the phase the ack gate waits for, so the
//! decision made here *is* the ack decision.

use ferrosa_common::accord::TxnId;
use ferrosa_storage::accord::sync_writer::{SyncWriteResult, SyncWriter};

/// Whether the apply path may acknowledge the apply — the journal's decision, and
/// the reason it is a named value rather than `Result<(), ()>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppliedAck {
    /// The applied decision is durable on this replica. Advance the transaction to
    /// `Applied`; the apply ack gate reads that phase, so this is the acknowledgement.
    Ack,
    /// Nothing durable records the apply. Leave the transaction undecided so the
    /// per-transaction Apply retry re-drives it, and withhold the ack.
    Withhold,
}

/// Write the protocol-log `Applied` marker for `txn_id` and decide whether this
/// replica may acknowledge the apply.
///
/// `rows_durable` is what the caller knows about the storage half of the apply:
///
/// * `true` — the applier wrote the rows and returned `Ok`, so they are durable
///   and readable. A marker fsync failure is then a gap in the journal's record
///   only, and it must NOT withhold the ack: the row is on disk, so denying the
///   apply is a lie about durability, and the coordinator's `TxnAbandoned` on the
///   strength of that denial is the same lie told to the client (G1).
/// * `false` — a no-write finalize wrote no rows, so the marker IS the only
///   durable record of the apply. A failed fsync withholds the ack, and the
///   transaction stays put for its retry. This half is the original contract and
///   is deliberately unchanged.
pub(crate) fn record_applied_marker(
    writer: &dyn SyncWriter,
    txn_id: TxnId,
    rows_durable: bool,
) -> AppliedAck {
    // The marker body is unchanged from the inline write it replaces, so an
    // existing protocol log replays identically.
    let data = format!("Applied:{}", txn_id.0.time);
    match writer.write_and_sync(data.as_bytes()) {
        SyncWriteResult::FsyncFailed(e) if rows_durable => {
            tracing::error!(
                txn_id = ?txn_id, error = %e,
                "accord: Applied marker fsync failed after a durable row write — the rows are \
                 durable and readable, so the apply is acknowledged; crash replay re-drives the \
                 marker idempotently"
            );
            AppliedAck::Ack
        }
        SyncWriteResult::FsyncFailed(e) => {
            tracing::error!(
                txn_id = ?txn_id, error = %e,
                "accord: Applied marker fsync failed — txn stays Committed until its Apply is retried"
            );
            AppliedAck::Withhold
        }
        _ => AppliedAck::Ack,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::accord::Timestamp;

    fn txn(micros: u64) -> TxnId {
        TxnId::new(1, Timestamp::synthetic(micros))
    }

    /// Control: a journal write that succeeds acknowledges, with or without durable
    /// rows. Without this, "withhold" could be a function that never acks anything.
    #[test]
    fn a_successful_marker_acks() {
        let writer = ferrosa_storage::accord::sync_writer::MockSyncWriter::new();
        assert_eq!(
            record_applied_marker(&writer, txn(1), true),
            AppliedAck::Ack
        );
        assert_eq!(
            record_applied_marker(&writer, txn(2), false),
            AppliedAck::Ack
        );
    }

    /// The G1 half: rows already durable ⇒ the marker's failure must not withhold the
    /// ack. This is the decision the handler-level hazard test depends on.
    #[test]
    fn a_failed_marker_still_acks_when_the_rows_are_durable() {
        let writer = ferrosa_storage::accord::sync_writer::MockSyncWriter::new();
        writer.set_fsync_failure(true);
        assert_eq!(
            record_applied_marker(&writer, txn(1), true),
            AppliedAck::Ack,
            "durable rows must win over the marker's fsync failure — otherwise the replica \
             denies an apply it actually performed (G1)"
        );
    }

    /// The unchanged half: with no durable row write the marker is the only durable
    /// record, so its failure must withhold the ack (never falsely `Applied`).
    #[test]
    fn a_failed_marker_withholds_when_nothing_durable_was_written() {
        let writer = ferrosa_storage::accord::sync_writer::MockSyncWriter::new();
        writer.set_fsync_failure(true);
        assert_eq!(
            record_applied_marker(&writer, txn(1), false),
            AppliedAck::Withhold,
            "a no-write finalize has no durable row, so a failed marker must withhold the ack"
        );
    }
}
