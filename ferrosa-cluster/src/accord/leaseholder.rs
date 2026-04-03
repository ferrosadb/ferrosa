//! Leaseholder assignment for Accord transaction coordination.
//!
//! A **leaseholder** is the node that coordinates Accord transactions for a
//! given token range during a particular epoch. Leaseholder assignment is
//! derived deterministically from the token ring: the first replica for a
//! token is the leaseholder. On node failure, the next live replica takes
//! over. Stale assignments are detected by epoch mismatch, and a node must
//! pass a local conflict check before claiming the lease.

use crate::raft::{NodeState, Token};
use crate::ring::TokenRing;
use ferrosa_storage::accord::conflict_index::ConflictIndex;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A leaseholder assignment for a token range in a given epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseAssignment {
    /// The node that holds the lease.
    pub node_id: u64,
    /// The token this lease covers.
    pub token: Token,
    /// The epoch in which this assignment is valid.
    pub epoch: u64,
}

/// Errors that can occur during leaseholder operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseError {
    /// No live replicas available for the token.
    NoLiveReplicas,
    /// The lease assignment has a stale epoch.
    StaleEpoch {
        assignment_epoch: u64,
        current_epoch: u64,
    },
    /// Local conflict index has in-flight transactions that prevent claiming.
    LocalConflict { in_flight_count: usize },
}

impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseError::NoLiveReplicas => write!(f, "no live replicas for token"),
            LeaseError::StaleEpoch {
                assignment_epoch,
                current_epoch,
            } => write!(
                f,
                "stale lease epoch: assignment={}, current={}",
                assignment_epoch, current_epoch
            ),
            LeaseError::LocalConflict { in_flight_count } => write!(
                f,
                "local conflict: {} in-flight transactions",
                in_flight_count
            ),
        }
    }
}

impl std::error::Error for LeaseError {}

// ---------------------------------------------------------------------------
// Leaseholder manager
// ---------------------------------------------------------------------------

/// Manages leaseholder assignments for Accord transaction coordination.
///
/// Assignments are deterministic: the first live replica in the token ring
/// for a given token is the leaseholder. The manager tracks the current
/// epoch and detects stale assignments.
pub struct LeaseholderManager {
    /// Current cluster epoch. Incremented on topology changes.
    current_epoch: u64,
    /// Replication factor used for replica selection.
    replication_factor: usize,
}

impl LeaseholderManager {
    /// Create a new leaseholder manager.
    ///
    /// # Panics
    ///
    /// Panics if `replication_factor` is zero.
    pub fn new(replication_factor: usize) -> Self {
        assert!(
            replication_factor > 0,
            "replication_factor must be positive"
        );
        Self {
            current_epoch: 1,
            replication_factor,
        }
    }

    /// Assign a leaseholder for the given token using the token ring.
    ///
    /// The leaseholder is the first replica in the ring that is in
    /// `NodeState::Normal`. Returns `LeaseError::NoLiveReplicas` if
    /// no replicas are in Normal state.
    pub fn assign(&self, ring: &TokenRing, token: Token) -> Result<LeaseAssignment, LeaseError> {
        let replicas = ring.replicas(token, self.replication_factor);
        for node_id in replicas {
            if let Some(info) = ring.get_node(node_id) {
                if info.state == NodeState::Normal {
                    return Ok(LeaseAssignment {
                        node_id,
                        token,
                        epoch: self.current_epoch,
                    });
                }
            }
        }
        Err(LeaseError::NoLiveReplicas)
    }

    /// Reassign the leaseholder after a node failure.
    ///
    /// Marks the failed node as `Leaving` in the ring, increments the epoch,
    /// and assigns the next live replica. The caller must provide a mutable
    /// reference to the ring so the node state can be updated.
    pub fn failover(
        &mut self,
        ring: &mut TokenRing,
        token: Token,
        failed_node_id: u64,
    ) -> Result<LeaseAssignment, LeaseError> {
        ring.set_node_state(failed_node_id, NodeState::Leaving);
        self.current_epoch += 1;
        self.assign(ring, token)
    }

    /// Validate that a lease assignment is not stale.
    ///
    /// Returns `Err(LeaseError::StaleEpoch)` if the assignment's epoch
    /// does not match the current epoch.
    pub fn validate_epoch(&self, assignment: &LeaseAssignment) -> Result<(), LeaseError> {
        if assignment.epoch != self.current_epoch {
            return Err(LeaseError::StaleEpoch {
                assignment_epoch: assignment.epoch,
                current_epoch: self.current_epoch,
            });
        }
        Ok(())
    }

    /// Check whether the local node can claim the lease by verifying
    /// the conflict index has no in-flight transactions for the given key.
    ///
    /// A node must not claim a lease while it has unresolved in-flight
    /// transactions that could conflict with the new lease scope.
    pub fn check_local_conflicts(
        &self,
        conflict_index: &ConflictIndex,
        key: &[u8],
    ) -> Result<(), LeaseError> {
        if let Some(_ts) = conflict_index.max_conflicting_timestamp(key) {
            // There are in-flight transactions on this key.
            // Count them via deps_before_t0 with a far-future timestamp.
            let far_future = ferrosa_common::accord::Timestamp {
                epoch: u64::MAX,
                time: u64::MAX,
                seq: u32::MAX,
                node: u64::MAX,
            };
            let deps = conflict_index.deps_before_t0(key, &far_future);
            return Err(LeaseError::LocalConflict {
                in_flight_count: deps.len(),
            });
        }
        Ok(())
    }

    /// Get the current epoch.
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    /// Advance the epoch (e.g., after a topology change).
    pub fn advance_epoch(&mut self) {
        self.current_epoch += 1;
    }
}

// ===========================================================================
// Tests — A4.5 (5 tests)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{NodeInfo, NodeState};
    use ferrosa_storage::accord::conflict_index::{InFlightWrite, TxnStatus};
    use uuid::Uuid;

    fn make_node(addr: &str) -> NodeInfo {
        NodeInfo {
            host_id: Uuid::new_v4(),
            addr: addr.to_string(),
            data_center: "dc1".to_string(),
            rack: "rack1".to_string(),
            state: NodeState::Normal,
            cql_broadcast: None,
        }
    }

    fn ts(time: u64) -> ferrosa_common::accord::Timestamp {
        ferrosa_common::accord::Timestamp {
            epoch: 0,
            time,
            seq: 0,
            node: 1,
        }
    }

    fn txn(time: u64) -> ferrosa_common::accord::TxnId {
        ferrosa_common::accord::TxnId(ts(time))
    }

    fn setup_ring() -> TokenRing {
        let mut ring = TokenRing::new();
        ring.add_node(1, make_node("10.0.0.1:7000"));
        ring.add_node(2, make_node("10.0.0.2:7000"));
        ring.add_node(3, make_node("10.0.0.3:7000"));
        ring.assign_tokens(1, &[100]);
        ring.assign_tokens(2, &[200]);
        ring.assign_tokens(3, &[300]);
        ring
    }

    // -----------------------------------------------------------------------
    // Test 1: leaseholder_assignment_from_token_ring
    // -----------------------------------------------------------------------

    #[test]
    fn leaseholder_assignment_from_token_ring() {
        let ring = setup_ring();
        let mgr = LeaseholderManager::new(3);

        // For token 50, the first replica walking clockwise from 50 is
        // node 1 (at token 100). Node 1 is Normal, so it gets the lease.
        let assignment = mgr.assign(&ring, 50).expect("should assign");
        assert_eq!(assignment.node_id, 1);
        assert_eq!(assignment.token, 50);
        assert_eq!(assignment.epoch, 1);

        // For token 150, the first replica is node 2 (at token 200).
        let assignment2 = mgr.assign(&ring, 150).expect("should assign");
        assert_eq!(assignment2.node_id, 2);
    }

    // -----------------------------------------------------------------------
    // Test 2: leaseholder_failover_update
    // -----------------------------------------------------------------------

    #[test]
    fn leaseholder_failover_update() {
        let mut ring = setup_ring();
        let mut mgr = LeaseholderManager::new(3);

        // Initial assignment: node 1 holds lease for token 50.
        let initial = mgr.assign(&ring, 50).expect("should assign");
        assert_eq!(initial.node_id, 1);
        assert_eq!(initial.epoch, 1);

        // Node 1 fails. Failover should reassign to node 2.
        let failover = mgr.failover(&mut ring, 50, 1).expect("should failover");
        assert_ne!(failover.node_id, 1, "failed node must not hold lease");
        assert_eq!(failover.epoch, 2, "epoch must advance on failover");

        // The new leaseholder should be a different live node.
        let new_node_info = ring.get_node(failover.node_id).unwrap();
        assert_eq!(new_node_info.state, NodeState::Normal);
    }

    // -----------------------------------------------------------------------
    // Test 3: stale_leaseholder_detection
    // -----------------------------------------------------------------------

    #[test]
    fn stale_leaseholder_detection() {
        let ring = setup_ring();
        let mut mgr = LeaseholderManager::new(3);

        // Get an assignment at epoch 1.
        let assignment = mgr.assign(&ring, 50).expect("should assign");
        assert_eq!(assignment.epoch, 1);

        // Validate passes at the current epoch.
        assert!(mgr.validate_epoch(&assignment).is_ok());

        // Advance epoch (simulating topology change).
        mgr.advance_epoch();
        assert_eq!(mgr.current_epoch(), 2);

        // The old assignment is now stale.
        let result = mgr.validate_epoch(&assignment);
        assert!(result.is_err());
        match result.unwrap_err() {
            LeaseError::StaleEpoch {
                assignment_epoch,
                current_epoch,
            } => {
                assert_eq!(assignment_epoch, 1);
                assert_eq!(current_epoch, 2);
            }
            other => panic!("expected StaleEpoch, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Test 4: leaseholder_local_conflict_check
    // -----------------------------------------------------------------------

    #[test]
    fn leaseholder_local_conflict_check() {
        let mgr = LeaseholderManager::new(3);
        let mut conflict_index = ConflictIndex::new(100);
        let key = b"partition-1";

        // No conflicts: check should pass.
        assert!(mgr.check_local_conflicts(&conflict_index, key).is_ok());

        // Register an in-flight transaction.
        let entry = InFlightWrite {
            txn_id: txn(100),
            t0: ts(100),
            accord_ts: None,
            status: TxnStatus::PreAccepted,
        };
        conflict_index.register(key, entry).unwrap();

        // Now check should fail with LocalConflict.
        let result = mgr.check_local_conflicts(&conflict_index, key);
        assert!(result.is_err());
        match result.unwrap_err() {
            LeaseError::LocalConflict { in_flight_count } => {
                assert_eq!(in_flight_count, 1);
            }
            other => panic!("expected LocalConflict, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Test 5: leaseholder_epoch_bounded
    // -----------------------------------------------------------------------

    #[test]
    fn leaseholder_epoch_bounded() {
        let ring = setup_ring();
        let mut mgr = LeaseholderManager::new(3);

        // Assignments at different epochs are distinct.
        let a1 = mgr.assign(&ring, 50).expect("should assign");
        assert_eq!(a1.epoch, 1);

        mgr.advance_epoch();
        let a2 = mgr.assign(&ring, 50).expect("should assign");
        assert_eq!(a2.epoch, 2);

        // a1 is stale relative to a2's epoch.
        assert!(mgr.validate_epoch(&a1).is_err());

        // a2 is valid at the current epoch.
        assert!(mgr.validate_epoch(&a2).is_ok());

        // Advance again — both are now stale.
        mgr.advance_epoch();
        assert!(mgr.validate_epoch(&a1).is_err());
        assert!(mgr.validate_epoch(&a2).is_err());

        // Only a new assignment at epoch 3 is valid.
        let a3 = mgr.assign(&ring, 50).expect("should assign");
        assert_eq!(a3.epoch, 3);
        assert!(mgr.validate_epoch(&a3).is_ok());
    }
}
