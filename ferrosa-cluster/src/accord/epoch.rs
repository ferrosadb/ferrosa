//! Epoch propagation for Accord protocol messages.
//!
//! Every Accord protocol message carries an epoch field. When a replica
//! receives a message with a different epoch than its own, it must handle
//! the mismatch appropriately:
//!
//! - **Higher epoch**: The sender knows about a configuration change this
//!   replica hasn't seen yet. Trigger an epoch sync.
//! - **Lower epoch**: The sender is behind. Include the current epoch in
//!   the response so the sender can catch up.
//! - **Equal epoch**: Normal fast path — no sync needed.
//!
//! When all replicas in a quorum report epoch mismatch, the node forces
//! an epoch sync before proceeding.

use std::collections::HashMap;

use ferrosa_common::accord::TxnId;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// The epoch carried by every Accord protocol message.
pub type Epoch = u64;

/// A protocol message with epoch annotation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochMessage {
    /// Sender's current epoch.
    pub epoch: Epoch,
    /// Sender node ID.
    pub sender: u64,
    /// Transaction this message pertains to.
    pub txn_id: TxnId,
    /// The message payload type.
    pub kind: MessageKind,
}

/// Enumeration of message types that carry epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    PreAccept,
    PreAcceptOK,
    Accept,
    AcceptOK,
    Commit,
    Apply,
    Recover,
    RecoverOK,
    Nack,
}

/// Result of epoch validation on an incoming message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpochDecision {
    /// Epochs match — proceed on the fast path.
    FastPath,
    /// Sender has a higher epoch — we need to sync.
    SyncRequired {
        local_epoch: Epoch,
        remote_epoch: Epoch,
    },
    /// Sender has a lower epoch — include our epoch in the response.
    StaleRemote {
        local_epoch: Epoch,
        remote_epoch: Epoch,
    },
}

/// Result of checking quorum-wide epoch responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuorumEpochCheck {
    /// All replicas agree on the epoch.
    Consistent,
    /// Some replicas have a different epoch — slow path needed.
    SlowPath { mismatched: Vec<(u64, Epoch)> },
    /// ALL replicas have a different epoch — force epoch sync.
    ForceSync { expected: Epoch },
}

// ---------------------------------------------------------------------------
// EpochTracker
// ---------------------------------------------------------------------------

/// Tracks the local epoch and validates incoming message epochs.
///
/// Thread-safety: designed for single-threaded shard executor access.
pub struct EpochTracker {
    /// This node's current epoch.
    local_epoch: Epoch,
    /// This node's ID.
    node_id: u64,
    /// Epoch responses from replicas for a given transaction round.
    /// Maps txn_id -> (node_id -> epoch).
    quorum_epochs: HashMap<TxnId, HashMap<u64, Epoch>>,
    /// Number of replicas expected in each quorum.
    quorum_size: usize,
}

impl EpochTracker {
    /// Create a new tracker at the given epoch.
    ///
    /// # Panics
    /// Panics if `quorum_size` is zero.
    pub fn new(node_id: u64, initial_epoch: Epoch, quorum_size: usize) -> Self {
        assert!(quorum_size > 0, "quorum_size must be positive");
        Self {
            local_epoch: initial_epoch,
            node_id,
            quorum_epochs: HashMap::new(),
            quorum_size,
        }
    }

    /// Current local epoch.
    pub fn local_epoch(&self) -> Epoch {
        self.local_epoch
    }

    /// Node ID.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Expected quorum size.
    pub fn quorum_size(&self) -> usize {
        self.quorum_size
    }

    /// Advance the local epoch.
    ///
    /// # Panics
    /// Panics if `new_epoch <= local_epoch` (epochs must advance forward).
    pub fn advance_epoch(&mut self, new_epoch: Epoch) {
        assert!(
            new_epoch > self.local_epoch,
            "epoch must advance: {} -> {}",
            self.local_epoch,
            new_epoch
        );
        self.local_epoch = new_epoch;
    }

    /// Stamp an outgoing message with the local epoch.
    pub fn stamp_message(&self, txn_id: TxnId, kind: MessageKind) -> EpochMessage {
        EpochMessage {
            epoch: self.local_epoch,
            sender: self.node_id,
            txn_id,
            kind,
        }
    }

    /// Validate an incoming message's epoch against the local epoch.
    pub fn validate(&self, message: &EpochMessage) -> EpochDecision {
        if message.epoch == self.local_epoch {
            EpochDecision::FastPath
        } else if message.epoch > self.local_epoch {
            EpochDecision::SyncRequired {
                local_epoch: self.local_epoch,
                remote_epoch: message.epoch,
            }
        } else {
            EpochDecision::StaleRemote {
                local_epoch: self.local_epoch,
                remote_epoch: message.epoch,
            }
        }
    }

    /// Record a replica's epoch response for quorum-wide checking.
    pub fn record_quorum_epoch(&mut self, txn_id: TxnId, node_id: u64, epoch: Epoch) {
        self.quorum_epochs
            .entry(txn_id)
            .or_default()
            .insert(node_id, epoch);
    }

    /// Check whether quorum responses show consistent epochs.
    ///
    /// Must be called after collecting responses from at least `quorum_size`
    /// replicas.
    pub fn check_quorum_epochs(&self, txn_id: &TxnId) -> QuorumEpochCheck {
        let responses = match self.quorum_epochs.get(txn_id) {
            Some(r) => r,
            None => return QuorumEpochCheck::Consistent,
        };

        let mismatched: Vec<(u64, Epoch)> = responses
            .iter()
            .filter(|(_, &epoch)| epoch != self.local_epoch)
            .map(|(&node, &epoch)| (node, epoch))
            .collect();

        if mismatched.is_empty() {
            QuorumEpochCheck::Consistent
        } else if mismatched.len() >= responses.len() && !responses.is_empty() {
            // ALL replicas disagree — force sync.
            QuorumEpochCheck::ForceSync {
                expected: self.local_epoch,
            }
        } else {
            QuorumEpochCheck::SlowPath { mismatched }
        }
    }

    /// Clear quorum tracking for a completed transaction.
    pub fn clear_quorum(&mut self, txn_id: &TxnId) {
        self.quorum_epochs.remove(txn_id);
    }
}

// ===========================================================================
// Tests — 3 tests for A7.2
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::accord::Timestamp;

    fn ts(time: u64) -> Timestamp {
        Timestamp {
            epoch: 0,
            time,
            seq: 0,
            node: 0,
        }
    }

    fn txn(time: u64) -> TxnId {
        TxnId(ts(time))
    }

    // -----------------------------------------------------------------------
    // Test 1: epoch_propagation_all_messages
    //   Every message carries epoch — verify stamp and validate.
    // -----------------------------------------------------------------------

    #[test]
    fn epoch_propagation_all_messages() {
        let tracker = EpochTracker::new(1, 5, 3);

        // All message kinds must carry the epoch.
        let kinds = [
            MessageKind::PreAccept,
            MessageKind::PreAcceptOK,
            MessageKind::Accept,
            MessageKind::AcceptOK,
            MessageKind::Commit,
            MessageKind::Apply,
            MessageKind::Recover,
            MessageKind::RecoverOK,
            MessageKind::Nack,
        ];

        let txn_id = txn(1000);

        for kind in &kinds {
            let msg = tracker.stamp_message(txn_id, *kind);
            assert_eq!(msg.epoch, 5, "message {:?} must carry local epoch 5", kind);
            assert_eq!(msg.sender, 1, "sender must be node 1");
            assert_eq!(msg.txn_id, txn_id, "txn_id must match");
            assert_eq!(&msg.kind, kind, "kind must match");

            // Validate our own message — should be FastPath.
            let decision = tracker.validate(&msg);
            assert_eq!(
                decision,
                EpochDecision::FastPath,
                "own message must be FastPath for {:?}",
                kind
            );
        }

        // Verify message from a node with higher epoch triggers SyncRequired.
        let remote_msg = EpochMessage {
            epoch: 7,
            sender: 2,
            txn_id,
            kind: MessageKind::PreAcceptOK,
        };
        let decision = tracker.validate(&remote_msg);
        assert_eq!(
            decision,
            EpochDecision::SyncRequired {
                local_epoch: 5,
                remote_epoch: 7,
            },
            "higher remote epoch must trigger SyncRequired"
        );

        // Message from a node with lower epoch triggers StaleRemote.
        let stale_msg = EpochMessage {
            epoch: 3,
            sender: 3,
            txn_id,
            kind: MessageKind::AcceptOK,
        };
        let decision = tracker.validate(&stale_msg);
        assert_eq!(
            decision,
            EpochDecision::StaleRemote {
                local_epoch: 5,
                remote_epoch: 3,
            },
            "lower remote epoch must trigger StaleRemote"
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: epoch_mismatch_slow_path_fallback
    //   Epoch mismatch from some replicas triggers slow path.
    // -----------------------------------------------------------------------

    #[test]
    fn epoch_mismatch_slow_path_fallback() {
        let mut tracker = EpochTracker::new(1, 5, 3);
        let txn_id = txn(1000);

        // Two replicas respond with epoch 5 (matching), one with epoch 4.
        tracker.record_quorum_epoch(txn_id, 2, 5);
        tracker.record_quorum_epoch(txn_id, 3, 5);
        tracker.record_quorum_epoch(txn_id, 4, 4); // stale

        let check = tracker.check_quorum_epochs(&txn_id);
        match check {
            QuorumEpochCheck::SlowPath { mismatched } => {
                assert_eq!(mismatched.len(), 1, "one replica mismatched");
                assert_eq!(mismatched[0].0, 4, "node 4 is the mismatched replica");
                assert_eq!(mismatched[0].1, 4, "node 4's epoch is 4");
            }
            other => panic!("expected SlowPath, got {:?}", other),
        }

        // Clean up.
        tracker.clear_quorum(&txn_id);
        let after = tracker.check_quorum_epochs(&txn_id);
        assert_eq!(after, QuorumEpochCheck::Consistent, "cleared quorum");
    }

    // -----------------------------------------------------------------------
    // Test 3: epoch_mismatch_all_replicas
    //   All replicas report mismatch — force epoch sync.
    // -----------------------------------------------------------------------

    #[test]
    fn epoch_mismatch_all_replicas() {
        let mut tracker = EpochTracker::new(1, 5, 3);
        let txn_id = txn(2000);

        // ALL replicas respond with a different epoch than ours.
        tracker.record_quorum_epoch(txn_id, 2, 6);
        tracker.record_quorum_epoch(txn_id, 3, 7);
        tracker.record_quorum_epoch(txn_id, 4, 6);

        let check = tracker.check_quorum_epochs(&txn_id);
        match check {
            QuorumEpochCheck::ForceSync { expected } => {
                assert_eq!(
                    expected, 5,
                    "force sync expected epoch must be our local epoch"
                );
            }
            other => panic!("expected ForceSync, got {:?}", other),
        }

        // After advancing our epoch and re-checking with matching responses,
        // should be Consistent.
        tracker.advance_epoch(7);
        assert_eq!(tracker.local_epoch(), 7);
        tracker.clear_quorum(&txn_id);
        tracker.record_quorum_epoch(txn_id, 2, 7);
        tracker.record_quorum_epoch(txn_id, 3, 7);
        tracker.record_quorum_epoch(txn_id, 4, 7);

        let check2 = tracker.check_quorum_epochs(&txn_id);
        assert_eq!(
            check2,
            QuorumEpochCheck::Consistent,
            "after sync, quorum must be consistent"
        );
    }
}
