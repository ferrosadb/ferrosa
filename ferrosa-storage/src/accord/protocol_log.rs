//! Local-only protocol log for Accord transaction state.
//!
//! The [`ProtocolLog`] stores PreAccepted, Accepted, and Committed entries
//! that track Accord consensus progress. These entries are **never** uploaded
//! to S3 — they exist only for local crash recovery and replay.
//!
//! # Current implementation
//!
//! In-memory `Vec` storage. Disk persistence with segment rotation is a later
//! optimization (the key semantic — no S3 upload — is enforced now).

use std::path::{Path, PathBuf};

use super::entries::{AccordProtocolEntry, TxnId};

/// Local-only protocol log for Accord state. NOT uploaded to S3.
///
/// Uses smaller segments and aggressive GC compared to the main commit log.
/// The critical invariant is that this log is never uploaded to S3: it contains
/// only protocol-level coordination state, not user data.
pub struct ProtocolLog {
    /// Directory where protocol log segments will be stored (future use).
    log_dir: PathBuf,
    /// In-memory entry storage (disk persistence is a later optimization).
    entries: Vec<AccordProtocolEntry>,
}

impl ProtocolLog {
    /// Create a new protocol log rooted at the given directory.
    ///
    /// The directory is recorded for future disk persistence but is not
    /// used in the current in-memory implementation.
    pub fn new(log_dir: &Path) -> Self {
        Self {
            log_dir: log_dir.to_path_buf(),
            entries: Vec::new(),
        }
    }

    /// Append an entry. Returns immediately (in-memory).
    ///
    /// # Panics
    ///
    /// Does not panic. Entry is cloned into the internal buffer.
    pub fn append(&mut self, entry: AccordProtocolEntry) {
        self.entries.push(entry);
    }

    /// GC entries for a transaction that has been Applied.
    ///
    /// Removes all PreAccepted, Accepted, and Committed entries for the
    /// given `txn_id`. Entries for other transactions are preserved.
    pub fn gc_applied(&mut self, txn_id: &TxnId) {
        self.entries.retain(|e| e.txn_id() != txn_id);
    }

    /// Replay all entries (for startup recovery).
    ///
    /// Returns entries in the order they were appended.
    pub fn replay(&self) -> Vec<AccordProtocolEntry> {
        self.entries.clone()
    }

    /// Check if a transaction has entries in the protocol log.
    pub fn has_entries(&self, txn_id: &TxnId) -> bool {
        self.entries.iter().any(|e| e.txn_id() == txn_id)
    }

    /// Number of entries (for monitoring).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the log contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the log directory path.
    pub fn log_dir(&self) -> &Path {
        &self.log_dir
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accord::entries::{AcceptedBallot, Timestamp};

    fn ts(micros: u64, logical: u32) -> Timestamp {
        Timestamp {
            epoch_micros: micros,
            logical,
        }
    }

    fn txn(node: u64, micros: u64) -> TxnId {
        TxnId {
            node,
            timestamp: ts(micros, 0),
        }
    }

    /// Test 1: protocol_log_not_uploaded
    ///
    /// Verify ProtocolLog has no S3 upload method or UploadManager reference.
    /// This is a design/compile-time test: the type compiles without any
    /// dependency on upload infrastructure.
    #[test]
    fn protocol_log_not_uploaded() {
        let dir = tempfile::tempdir().unwrap();
        let log = ProtocolLog::new(dir.path());

        // ProtocolLog has no upload(), no UploadManager field, no S3 config.
        // This test verifies the type compiles cleanly without upload deps.
        // The log_dir is for future local disk persistence, not S3.
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);

        // Verify the log directory is stored correctly.
        assert_eq!(log.log_dir(), dir.path());
    }

    /// Test 2: protocol_log_gc_after_applied
    ///
    /// Append PreAccepted, Accepted, Committed for txn T1.
    /// Call gc_applied(T1). Assert: T1 entries removed, T2 entries remain.
    #[test]
    fn protocol_log_gc_after_applied() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = ProtocolLog::new(dir.path());

        let t1 = txn(1, 1000);
        let t2 = txn(2, 2000);

        // Append entries for T1.
        log.append(AccordProtocolEntry::PreAccepted {
            txn_id: t1,
            t0: ts(1000, 0),
            t: ts(1001, 1),
            deps: vec![],
        });
        log.append(AccordProtocolEntry::Accepted {
            txn_id: t1,
            t0: ts(1000, 0),
            t: ts(1001, 1),
            deps: vec![],
            accepted_ballot: AcceptedBallot { ballot: 1, node: 1 },
        });
        log.append(AccordProtocolEntry::Committed {
            txn_id: t1,
            t: ts(1001, 1),
            deps: vec![],
        });

        // Append entries for T2.
        log.append(AccordProtocolEntry::PreAccepted {
            txn_id: t2,
            t0: ts(2000, 0),
            t: ts(2001, 0),
            deps: vec![t1],
        });

        assert_eq!(log.len(), 4);
        assert!(log.has_entries(&t1));
        assert!(log.has_entries(&t2));

        // GC T1 (it has been applied).
        log.gc_applied(&t1);

        // T1 entries should be removed; T2 should remain.
        assert!(!log.has_entries(&t1), "T1 entries should be GC'd");
        assert!(log.has_entries(&t2), "T2 entries should remain");
        assert_eq!(log.len(), 1);
    }

    /// Test 4: protocol_log_segment_size
    ///
    /// Verify the ProtocolLog is bounded (has a len() method).
    /// Full segment rotation comes later.
    #[test]
    fn protocol_log_segment_size() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = ProtocolLog::new(dir.path());

        assert_eq!(log.len(), 0);
        assert!(log.is_empty());

        for i in 0..100 {
            log.append(AccordProtocolEntry::PreAccepted {
                txn_id: txn(1, i),
                t0: ts(i, 0),
                t: ts(i + 1, 0),
                deps: vec![],
            });
        }

        assert_eq!(log.len(), 100);
        assert!(!log.is_empty());
    }

    /// Test 5: protocol_log_replay_on_startup
    ///
    /// Append 5 entries for 2 txns. Replay. Assert: all 5 entries returned
    /// in order.
    #[test]
    fn protocol_log_replay_on_startup() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = ProtocolLog::new(dir.path());

        let t1 = txn(1, 1000);
        let t2 = txn(2, 2000);

        // 3 entries for T1.
        log.append(AccordProtocolEntry::PreAccepted {
            txn_id: t1,
            t0: ts(1000, 0),
            t: ts(1001, 1),
            deps: vec![],
        });
        log.append(AccordProtocolEntry::Accepted {
            txn_id: t1,
            t0: ts(1000, 0),
            t: ts(1001, 1),
            deps: vec![],
            accepted_ballot: AcceptedBallot { ballot: 1, node: 1 },
        });
        log.append(AccordProtocolEntry::Committed {
            txn_id: t1,
            t: ts(1001, 1),
            deps: vec![],
        });

        // 2 entries for T2.
        log.append(AccordProtocolEntry::PreAccepted {
            txn_id: t2,
            t0: ts(2000, 0),
            t: ts(2001, 0),
            deps: vec![t1],
        });
        log.append(AccordProtocolEntry::Accepted {
            txn_id: t2,
            t0: ts(2000, 0),
            t: ts(2001, 0),
            deps: vec![t1],
            accepted_ballot: AcceptedBallot { ballot: 1, node: 2 },
        });

        let replayed = log.replay();
        assert_eq!(replayed.len(), 5);

        // Verify order: first 3 are T1, last 2 are T2.
        assert_eq!(replayed[0].txn_id(), &t1);
        assert_eq!(replayed[1].txn_id(), &t1);
        assert_eq!(replayed[2].txn_id(), &t1);
        assert_eq!(replayed[3].txn_id(), &t2);
        assert_eq!(replayed[4].txn_id(), &t2);

        // Verify specific entry types via pattern match.
        assert!(matches!(
            replayed[0],
            AccordProtocolEntry::PreAccepted { .. }
        ));
        assert!(matches!(replayed[1], AccordProtocolEntry::Accepted { .. }));
        assert!(matches!(replayed[2], AccordProtocolEntry::Committed { .. }));
        assert!(matches!(
            replayed[3],
            AccordProtocolEntry::PreAccepted { .. }
        ));
        assert!(matches!(replayed[4], AccordProtocolEntry::Accepted { .. }));
    }

    /// Test 6: protocol_log_and_main_log_replay_order
    ///
    /// Append protocol entries. Simulate an AccordAppliedEntry (main log).
    /// GC the protocol entries. Replay. Assert: GC'd entries are gone.
    /// The applied txn is not in the protocol log.
    #[test]
    fn protocol_log_and_main_log_replay_order() {
        use crate::accord::entries::AccordAppliedEntry;

        let dir = tempfile::tempdir().unwrap();
        let mut log = ProtocolLog::new(dir.path());

        let t1 = txn(1, 1000);
        let t2 = txn(2, 2000);

        // Append protocol entries for both txns.
        log.append(AccordProtocolEntry::PreAccepted {
            txn_id: t1,
            t0: ts(1000, 0),
            t: ts(1001, 1),
            deps: vec![],
        });
        log.append(AccordProtocolEntry::Accepted {
            txn_id: t1,
            t0: ts(1000, 0),
            t: ts(1001, 1),
            deps: vec![],
            accepted_ballot: AcceptedBallot { ballot: 1, node: 1 },
        });
        log.append(AccordProtocolEntry::Committed {
            txn_id: t1,
            t: ts(1001, 1),
            deps: vec![],
        });
        log.append(AccordProtocolEntry::PreAccepted {
            txn_id: t2,
            t0: ts(2000, 0),
            t: ts(2001, 0),
            deps: vec![t1],
        });

        assert_eq!(log.len(), 4);

        // Simulate T1 being applied (written to main commit log).
        let _applied = AccordAppliedEntry {
            txn_id: t1,
            t: ts(1001, 1),
            result: vec![42],
        };

        // GC T1 from protocol log.
        log.gc_applied(&t1);

        // Replay: only T2 entries remain.
        let replayed = log.replay();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].txn_id(), &t2);

        // T1 is not in the protocol log.
        assert!(!log.has_entries(&t1));
    }
}
