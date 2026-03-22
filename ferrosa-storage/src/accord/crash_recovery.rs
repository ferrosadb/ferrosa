//! Accord crash recovery: replay persisted entries to reconstruct state.
//!
//! On startup, the node replays serialized [`AccordProtocolEntry`] and
//! [`AccordAppliedEntry`] records from the protocol log on disk. The
//! [`CrashRecoveryReplay`] struct drives this process:
//!
//! 1. Read raw bytes from the protocol log directory.
//! 2. Deserialize each entry, skipping any that fail CRC validation
//!    (partial writes from a crash mid-write).
//! 3. Reconstruct per-transaction state, the conflict index, and the
//!    set of already-applied transaction IDs.
//!
//! # Idempotency
//!
//! Transactions that reached the `Applied` state are recorded in
//! `applied_txn_ids`. The caller must check this set before re-applying
//! any transaction to avoid duplicate side effects.

use std::collections::{HashMap, HashSet};

use super::entries::{AccordAppliedEntry, AccordProtocolEntry, Timestamp, TxnId};

// ---------------------------------------------------------------------------
// Reconstructed state
// ---------------------------------------------------------------------------

/// Per-transaction state reconstructed from replayed entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayedTxnState {
    pub txn_id: TxnId,
    pub phase: ReplayedPhase,
    /// Original proposed timestamp (from PreAccepted).
    pub t0: Option<Timestamp>,
    /// Current / committed timestamp.
    pub t: Timestamp,
    /// Dependency set (union of all deps seen).
    pub deps: Vec<TxnId>,
}

/// Phase of a replayed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReplayedPhase {
    PreAccepted,
    Accepted,
    Committed,
    Applied,
}

/// Conflict index entry reconstructed from replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayedConflictEntry {
    pub txn_id: TxnId,
    pub t0: Timestamp,
    pub phase: ReplayedPhase,
}

// ---------------------------------------------------------------------------
// CrashRecoveryReplay
// ---------------------------------------------------------------------------

/// Drives Accord crash recovery by replaying persisted entries.
///
/// After calling `replay()`, the caller can inspect:
/// - `txn_states` — per-transaction consensus state
/// - `applied_txn_ids` — transactions that must NOT be re-applied
/// - `conflict_entries` — entries for rebuilding the ConflictIndex
/// - `skipped_count` — number of entries that failed deserialization
pub struct CrashRecoveryReplay {
    /// Reconstructed per-transaction state, keyed by TxnId.
    txn_states: HashMap<TxnId, ReplayedTxnState>,
    /// Transactions that reached Applied (must not be re-applied).
    applied_txn_ids: HashSet<TxnId>,
    /// Conflict index entries for rebuilding.
    conflict_entries: Vec<ReplayedConflictEntry>,
    /// Number of entries that failed deserialization (partial writes, corruption).
    skipped_count: usize,
}

impl CrashRecoveryReplay {
    /// Create a new empty replay state.
    pub fn new() -> Self {
        Self {
            txn_states: HashMap::new(),
            applied_txn_ids: HashSet::new(),
            conflict_entries: Vec::new(),
            skipped_count: 0,
        }
    }

    /// Replay a sequence of raw serialized entries.
    ///
    /// Each element in `raw_entries` is the serialized bytes of one entry
    /// (protocol or applied). Entries that fail CRC validation are silently
    /// skipped (counted in `skipped_count`).
    ///
    /// Protocol entries are tried first; if that fails (and it is not a CRC
    /// error on a valid-length entry), applied entry deserialization is
    /// attempted.
    pub fn replay(&mut self, raw_entries: &[Vec<u8>]) {
        for raw in raw_entries {
            // Try protocol entry first.
            if let Ok(entry) = AccordProtocolEntry::deserialize(raw) {
                self.ingest_protocol_entry(&entry);
                continue;
            }

            // Try applied entry.
            if let Ok(entry) = AccordAppliedEntry::deserialize(raw) {
                self.ingest_applied_entry(&entry);
                continue;
            }

            // Neither worked — count as skipped (partial write / corruption).
            self.skipped_count += 1;
        }
    }

    /// Ingest a deserialized protocol entry.
    fn ingest_protocol_entry(&mut self, entry: &AccordProtocolEntry) {
        let txn_id = *entry.txn_id();

        let (phase, t0, t, deps) = match entry {
            AccordProtocolEntry::PreAccepted {
                txn_id: _,
                t0,
                t,
                deps,
            } => (ReplayedPhase::PreAccepted, Some(*t0), *t, deps.clone()),
            AccordProtocolEntry::Accepted {
                txn_id: _,
                t0,
                t,
                deps,
                ..
            } => (ReplayedPhase::Accepted, Some(*t0), *t, deps.clone()),
            AccordProtocolEntry::Committed {
                txn_id: _, t, deps, ..
            } => (ReplayedPhase::Committed, None, *t, deps.clone()),
        };

        // Update or create txn state — only advance phase forward.
        let state = self
            .txn_states
            .entry(txn_id)
            .or_insert_with(|| ReplayedTxnState {
                txn_id,
                phase,
                t0,
                t,
                deps: deps.clone(),
            });

        if phase > state.phase {
            state.phase = phase;
            state.t = t;
            if !deps.is_empty() {
                state.deps = deps.clone();
            }
        }
        if t0.is_some() && state.t0.is_none() {
            state.t0 = t0;
        }

        // Add conflict entry (only for non-applied txns).
        if !self.applied_txn_ids.contains(&txn_id) {
            let conflict_t0 = t0.unwrap_or(t);
            self.conflict_entries.push(ReplayedConflictEntry {
                txn_id,
                t0: conflict_t0,
                phase,
            });
        }
    }

    /// Ingest a deserialized applied entry.
    fn ingest_applied_entry(&mut self, entry: &AccordAppliedEntry) {
        let txn_id = entry.txn_id;
        self.applied_txn_ids.insert(txn_id);

        // Update or create txn state at Applied phase.
        let state = self
            .txn_states
            .entry(txn_id)
            .or_insert_with(|| ReplayedTxnState {
                txn_id,
                phase: ReplayedPhase::Applied,
                t0: None,
                t: entry.t,
                deps: vec![],
            });
        state.phase = ReplayedPhase::Applied;

        // Remove conflict entries for applied txns — they are resolved.
        self.conflict_entries.retain(|e| e.txn_id != txn_id);
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// Returns the reconstructed per-transaction states.
    pub fn txn_states(&self) -> &HashMap<TxnId, ReplayedTxnState> {
        &self.txn_states
    }

    /// Returns the set of applied transaction IDs.
    pub fn applied_txn_ids(&self) -> &HashSet<TxnId> {
        &self.applied_txn_ids
    }

    /// Returns entries for rebuilding the ConflictIndex.
    pub fn conflict_entries(&self) -> &[ReplayedConflictEntry] {
        &self.conflict_entries
    }

    /// Returns the number of entries that were skipped due to corruption
    /// or partial writes.
    pub fn skipped_count(&self) -> usize {
        self.skipped_count
    }

    /// Returns true if any entries were successfully replayed.
    pub fn has_state(&self) -> bool {
        !self.txn_states.is_empty()
    }
}

impl Default for CrashRecoveryReplay {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accord::entries::{
        AcceptedBallot, AccordAppliedEntry, AccordProtocolEntry, Timestamp, TxnId,
    };

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

    /// Serialize a sequence of protocol and applied entries into raw bytes
    /// suitable for replay.
    fn serialize_entries(
        protocol: &[AccordProtocolEntry],
        applied: &[AccordAppliedEntry],
    ) -> Vec<Vec<u8>> {
        let mut raw = Vec::new();
        for entry in protocol {
            raw.push(entry.serialize());
        }
        for entry in applied {
            raw.push(entry.serialize());
        }
        raw
    }

    /// A5.3 Test 1: accord_crash_recovery_replay
    ///
    /// Reconstruct AccordStateMachine state from persisted entries.
    /// Append PreAccepted, Accepted, Committed for T1 and PreAccepted for T2.
    /// Replay. Assert: T1 is at Committed phase, T2 is at PreAccepted, both
    /// have correct timestamps and deps.
    #[test]
    fn accord_crash_recovery_replay() {
        let t1 = txn(1, 1000);
        let t2 = txn(2, 2000);

        let entries = vec![
            AccordProtocolEntry::PreAccepted {
                txn_id: t1,
                t0: ts(1000, 0),
                t: ts(1001, 1),
                deps: vec![],
            },
            AccordProtocolEntry::Accepted {
                txn_id: t1,
                t0: ts(1000, 0),
                t: ts(1001, 1),
                deps: vec![],
                accepted_ballot: AcceptedBallot { ballot: 1, node: 1 },
            },
            AccordProtocolEntry::Committed {
                txn_id: t1,
                t: ts(1001, 1),
                deps: vec![t2],
            },
            AccordProtocolEntry::PreAccepted {
                txn_id: t2,
                t0: ts(2000, 0),
                t: ts(2001, 0),
                deps: vec![t1],
            },
        ];

        let raw = serialize_entries(&entries, &[]);

        let mut replay = CrashRecoveryReplay::new();
        replay.replay(&raw);

        // T1 should be at Committed.
        let t1_state = replay.txn_states().get(&t1).expect("T1 should exist");
        assert_eq!(t1_state.phase, ReplayedPhase::Committed);
        assert_eq!(t1_state.t, ts(1001, 1));
        assert_eq!(t1_state.deps, vec![t2]);

        // T2 should be at PreAccepted.
        let t2_state = replay.txn_states().get(&t2).expect("T2 should exist");
        assert_eq!(t2_state.phase, ReplayedPhase::PreAccepted);
        assert_eq!(t2_state.t0, Some(ts(2000, 0)));
        assert_eq!(t2_state.t, ts(2001, 0));
        assert_eq!(t2_state.deps, vec![t1]);

        // No entries were skipped.
        assert_eq!(replay.skipped_count(), 0);

        // No transactions are applied.
        assert!(replay.applied_txn_ids().is_empty());

        // Both txns have conflict entries.
        assert!(replay.has_state());
        assert!(!replay.conflict_entries().is_empty());
    }

    /// A5.3 Test 2: replay_does_not_duplicate_apply
    ///
    /// Replay entries including an AccordAppliedEntry for T1.
    /// Assert: T1 is in applied_txn_ids and will not be re-applied.
    /// T2 (not applied) is still in txn_states at its protocol phase.
    #[test]
    fn replay_does_not_duplicate_apply() {
        let t1 = txn(1, 1000);
        let t2 = txn(2, 2000);

        let protocol = vec![
            AccordProtocolEntry::PreAccepted {
                txn_id: t1,
                t0: ts(1000, 0),
                t: ts(1001, 1),
                deps: vec![],
            },
            AccordProtocolEntry::Committed {
                txn_id: t1,
                t: ts(1001, 1),
                deps: vec![],
            },
            AccordProtocolEntry::PreAccepted {
                txn_id: t2,
                t0: ts(2000, 0),
                t: ts(2001, 0),
                deps: vec![t1],
            },
        ];

        let applied = vec![AccordAppliedEntry {
            txn_id: t1,
            t: ts(1001, 1),
            result: vec![42],
        }];

        let raw = serialize_entries(&protocol, &applied);

        let mut replay = CrashRecoveryReplay::new();
        replay.replay(&raw);

        // T1 is applied — must not be re-applied.
        assert!(
            replay.applied_txn_ids().contains(&t1),
            "T1 should be in applied set"
        );

        // T1 state should be at Applied phase.
        let t1_state = replay.txn_states().get(&t1).expect("T1 should exist");
        assert_eq!(t1_state.phase, ReplayedPhase::Applied);

        // T2 is NOT applied — still at PreAccepted.
        assert!(
            !replay.applied_txn_ids().contains(&t2),
            "T2 should not be in applied set"
        );
        let t2_state = replay.txn_states().get(&t2).expect("T2 should exist");
        assert_eq!(t2_state.phase, ReplayedPhase::PreAccepted);

        // Conflict entries should NOT contain T1 (it is applied and resolved).
        let t1_conflicts: Vec<_> = replay
            .conflict_entries()
            .iter()
            .filter(|e| e.txn_id == t1)
            .collect();
        assert!(
            t1_conflicts.is_empty(),
            "Applied T1 should not be in conflict entries"
        );

        // Conflict entries SHOULD contain T2.
        let t2_conflicts: Vec<_> = replay
            .conflict_entries()
            .iter()
            .filter(|e| e.txn_id == t2)
            .collect();
        assert!(!t2_conflicts.is_empty(), "T2 should be in conflict entries");
    }

    /// A5.3 Test 3: replay_reconstructs_conflict_index
    ///
    /// Replay entries for 3 transactions at different phases.
    /// Assert: conflict_entries contains entries for non-applied txns
    /// with correct t0 values for ConflictIndex rebuilding.
    #[test]
    fn replay_reconstructs_conflict_index() {
        let t1 = txn(1, 1000);
        let t2 = txn(2, 2000);
        let t3 = txn(3, 3000);

        let protocol = vec![
            // T1: PreAccepted -> Accepted -> Committed
            AccordProtocolEntry::PreAccepted {
                txn_id: t1,
                t0: ts(1000, 0),
                t: ts(1001, 1),
                deps: vec![],
            },
            AccordProtocolEntry::Accepted {
                txn_id: t1,
                t0: ts(1000, 0),
                t: ts(1001, 1),
                deps: vec![],
                accepted_ballot: AcceptedBallot { ballot: 1, node: 1 },
            },
            AccordProtocolEntry::Committed {
                txn_id: t1,
                t: ts(1001, 1),
                deps: vec![],
            },
            // T2: PreAccepted only
            AccordProtocolEntry::PreAccepted {
                txn_id: t2,
                t0: ts(2000, 0),
                t: ts(2001, 0),
                deps: vec![t1],
            },
            // T3: PreAccepted -> Accepted
            AccordProtocolEntry::PreAccepted {
                txn_id: t3,
                t0: ts(3000, 0),
                t: ts(3001, 0),
                deps: vec![t1, t2],
            },
            AccordProtocolEntry::Accepted {
                txn_id: t3,
                t0: ts(3000, 0),
                t: ts(3001, 0),
                deps: vec![t1, t2],
                accepted_ballot: AcceptedBallot { ballot: 1, node: 3 },
            },
        ];

        let raw = serialize_entries(&protocol, &[]);

        let mut replay = CrashRecoveryReplay::new();
        replay.replay(&raw);

        // All three txns should have conflict entries.
        let conflict_txn_ids: HashSet<TxnId> =
            replay.conflict_entries().iter().map(|e| e.txn_id).collect();
        assert!(conflict_txn_ids.contains(&t1), "T1 should be in conflicts");
        assert!(conflict_txn_ids.contains(&t2), "T2 should be in conflicts");
        assert!(conflict_txn_ids.contains(&t3), "T3 should be in conflicts");

        // Verify t0 values for conflict entries.
        let t1_entry = replay
            .conflict_entries()
            .iter()
            .find(|e| e.txn_id == t1)
            .expect("T1 conflict entry");
        assert_eq!(t1_entry.t0, ts(1000, 0));

        let t2_entry = replay
            .conflict_entries()
            .iter()
            .find(|e| e.txn_id == t2)
            .expect("T2 conflict entry");
        assert_eq!(t2_entry.t0, ts(2000, 0));

        let t3_entry = replay
            .conflict_entries()
            .iter()
            .find(|e| e.txn_id == t3)
            .expect("T3 conflict entry");
        assert_eq!(t3_entry.t0, ts(3000, 0));

        // Verify phases.
        let t1_state = replay.txn_states().get(&t1).unwrap();
        assert_eq!(t1_state.phase, ReplayedPhase::Committed);

        let t2_state = replay.txn_states().get(&t2).unwrap();
        assert_eq!(t2_state.phase, ReplayedPhase::PreAccepted);

        let t3_state = replay.txn_states().get(&t3).unwrap();
        assert_eq!(t3_state.phase, ReplayedPhase::Accepted);
    }

    /// A5.3 Test 4: replay_with_partial_write
    ///
    /// Simulate a crash mid-write by providing truncated/corrupted bytes.
    /// Assert: partial entries are skipped, valid entries are replayed,
    /// skipped_count reflects the number of bad entries.
    #[test]
    fn replay_with_partial_write() {
        let t1 = txn(1, 1000);
        let t2 = txn(2, 2000);

        let valid_entry = AccordProtocolEntry::PreAccepted {
            txn_id: t1,
            t0: ts(1000, 0),
            t: ts(1001, 1),
            deps: vec![],
        };
        let valid_bytes = valid_entry.serialize();

        let another_valid = AccordProtocolEntry::Committed {
            txn_id: t2,
            t: ts(2001, 0),
            deps: vec![t1],
        };
        let another_valid_bytes = another_valid.serialize();

        // Simulate partial writes:
        // 1. Truncated entry (crash mid-write, only first 3 bytes written).
        let truncated = vec![0x01, 0x00, 0x00];

        // 2. Corrupted entry (full length but flipped bit in payload).
        let mut corrupted = valid_entry.serialize();
        corrupted[5] ^= 0xFF;

        // 3. Empty entry (zero bytes written before crash).
        let empty: Vec<u8> = vec![];

        let raw = vec![
            valid_bytes,
            truncated,
            corrupted,
            empty,
            another_valid_bytes,
        ];

        let mut replay = CrashRecoveryReplay::new();
        replay.replay(&raw);

        // Only the two valid entries should have been replayed.
        assert_eq!(
            replay.txn_states().len(),
            2,
            "Only 2 valid transactions should be replayed"
        );
        assert!(replay.txn_states().contains_key(&t1));
        assert!(replay.txn_states().contains_key(&t2));

        // 3 entries should have been skipped (truncated, corrupted, empty).
        assert_eq!(
            replay.skipped_count(),
            3,
            "3 corrupt/partial entries should be skipped"
        );
    }
}
