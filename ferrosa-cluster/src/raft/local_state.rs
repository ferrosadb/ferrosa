//! Classifying this node's persisted Raft state before openraft starts.
//!
//! Two durable facts have to agree for a node to restart on its own log: the
//! state machine's `last_applied` and the log store's `last_purged`. The
//! invariant is stated in `SledLogStore::last_purged_log_id` -- "entries can
//! only be purged after they've been applied and snapshotted" -- and until
//! 2026-08-20 nothing checked it.
//!
//! node3 restarted holding `last_applied = 2905` and `last_purged = 3065`.
//! Entries 2906..=3065 had been deleted from its log and never applied to its
//! state machine, so they existed nowhere on that node. openraft discovered
//! this deep inside re-apply and failed the only way it could:
//!
//! ```text
//! Failed to get log entries, expected index: [2906, 2970), got [None, None)
//! raft initialization failed (Fatal)
//! ```
//!
//! That message names an index range, not a cause, and the node then logged
//! `LazyRaft returned None` every 3.5 seconds forever without recovering.
//!
//! The gap was narrow: `recover_from_purge_point` already existed for the
//! neighbouring case, but guarded on `last_applied.is_none()`, so a
//! `last_applied` that was present *and behind* the purge point fell through
//! it untouched.

/// What this node's persisted Raft state can be used for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalRaftState {
    /// The log and state machine agree; start openraft on them.
    Usable,

    /// `last_applied` was lost (an OOM kill drops it to `None`) but a purge
    /// point survives. The purge point is a safe baseline: openraft would
    /// otherwise replay from index 0 into entries that are gone.
    NeedsPurgePointBaseline { purge_point: u64 },

    /// `last_applied` is *behind* `last_purged`. The entries in between were
    /// deleted before they were applied, so this node cannot reconstruct them
    /// and no local recovery exists. Only a snapshot from the leader can
    /// repair it.
    StrandedBehindPurge { last_applied: u64, last_purged: u64 },
}

impl LocalRaftState {
    /// Whether the local log is beyond repair from local state alone.
    pub fn is_stranded(self) -> bool {
        matches!(self, Self::StrandedBehindPurge { .. })
    }
}

/// Decide what the persisted state supports, before openraft reads it.
///
/// Deliberately does **not** clamp `last_applied` forward to the purge point
/// in the stranded case. Clamping would silence the error and let the node
/// start, while asserting that entries it never applied had been applied --
/// trading a loud failure for silent data loss. The stranded node has to get
/// the missing state from a peer.
pub fn classify_local_raft_state(
    last_applied: Option<u64>,
    last_purged: Option<u64>,
) -> LocalRaftState {
    let Some(last_purged) = last_purged else {
        // Nothing has been purged, so nothing can be missing.
        return LocalRaftState::Usable;
    };

    match last_applied {
        None => LocalRaftState::NeedsPurgePointBaseline {
            purge_point: last_purged,
        },
        Some(last_applied) if last_applied < last_purged => LocalRaftState::StrandedBehindPurge {
            last_applied,
            last_purged,
        },
        Some(_) => LocalRaftState::Usable,
    }
}

/// What a purge request is allowed to delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PurgeDecision {
    /// Delete entries up to and including this index, as asked.
    Purge { through: u64 },

    /// Delete less than asked. Everything above `through` is not yet durably
    /// applied, so deleting it would destroy the only copy on this node.
    Clamp { through: u64, requested: u64 },

    /// Delete nothing: no snapshot is known to be durable, so no entry is
    /// known to be safely reconstructable.
    Skip { requested: u64 },
}

/// Decide how far a purge may go, given what is *durably* applied.
///
/// openraft only asks to purge up to a snapshot it believes exists, so in a
/// correct system `requested <= durable_applied` always holds and this returns
/// `Purge` unchanged. It exists for the case where that belief and the disk
/// disagree -- which is how node3 was stranded on 2026-08-20.
///
/// Purging *less* than asked is always safe: the cost is disk, and the entries
/// are deleted on the next purge once the snapshot covering them is durable.
/// Purging *more* than is durable is unrecoverable, because the entries exist
/// nowhere else on this node. That asymmetry is the whole argument for
/// clamping rather than trusting the request.
pub fn purge_ceiling(requested: u64, durable_applied: Option<u64>) -> PurgeDecision {
    match durable_applied {
        // Nothing is known to be durably applied, so nothing is known to be
        // reconstructable. Keep the log.
        None => PurgeDecision::Skip { requested },
        Some(0) => PurgeDecision::Skip { requested },
        Some(applied) if requested <= applied => PurgeDecision::Purge { through: requested },
        Some(applied) => PurgeDecision::Clamp {
            through: applied,
            requested,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- purge_ceiling ----------------------------------------------------

    /// The normal case: the snapshot covers the purge point, so purge as asked.
    #[test]
    fn a_purge_covered_by_a_durable_snapshot_proceeds() {
        assert_eq!(
            purge_ceiling(3065, Some(3065)),
            PurgeDecision::Purge { through: 3065 }
        );
        assert_eq!(
            purge_ceiling(3000, Some(3065)),
            PurgeDecision::Purge { through: 3000 }
        );
    }

    /// The case that stranded node3: the request runs past what is durably
    /// applied. Deleting 2906..=3065 there removed the node's only copy of
    /// those entries. Clamp to what the snapshot actually covers.
    #[test]
    fn a_purge_past_durable_applied_is_clamped_not_obeyed() {
        assert_eq!(
            purge_ceiling(3065, Some(2905)),
            PurgeDecision::Clamp {
                through: 2905,
                requested: 3065,
            },
            "entries above the durable applied index exist nowhere else on \
             this node, so a purge may not delete them"
        );
    }

    /// No durable snapshot means no entry is known to be reconstructable.
    #[test]
    fn a_purge_with_no_durable_snapshot_deletes_nothing() {
        assert_eq!(
            purge_ceiling(500, None),
            PurgeDecision::Skip { requested: 500 }
        );
        assert_eq!(
            purge_ceiling(500, Some(0)),
            PurgeDecision::Skip { requested: 500 }
        );
    }

    /// Whatever a purge is permitted to delete, the surviving state must still
    /// classify as `Usable` -- the guard must not be able to create the strand
    /// it exists to prevent.
    #[test]
    fn no_permitted_purge_can_strand_this_node() {
        let durable_applied = 2905u64;
        for requested in 0u64..4000 {
            let permitted = match purge_ceiling(requested, Some(durable_applied)) {
                PurgeDecision::Purge { through } => through,
                PurgeDecision::Clamp { through, .. } => through,
                PurgeDecision::Skip { .. } => continue,
            };
            assert!(
                permitted <= durable_applied,
                "purge of {requested} was permitted to {permitted}, past the \
                 durable applied index {durable_applied}"
            );
            assert_eq!(
                classify_local_raft_state(Some(durable_applied), Some(permitted)),
                LocalRaftState::Usable,
                "a permitted purge to {permitted} left the node stranded"
            );
        }
    }

    /// The exact state node3 restarted in on 2026-08-20.
    #[test]
    fn last_applied_behind_the_purge_point_is_stranded() {
        assert_eq!(
            classify_local_raft_state(Some(2905), Some(3065)),
            LocalRaftState::StrandedBehindPurge {
                last_applied: 2905,
                last_purged: 3065,
            },
            "entries 2906..=3065 are purged and unapplied, so they exist \
             nowhere on this node"
        );
    }

    /// The boundary: applied exactly up to the purge point is healthy. Purging
    /// index N after applying index N is the normal, correct sequence.
    #[test]
    fn applied_exactly_to_the_purge_point_is_usable() {
        assert_eq!(
            classify_local_raft_state(Some(3065), Some(3065)),
            LocalRaftState::Usable
        );
    }

    #[test]
    fn applied_ahead_of_the_purge_point_is_usable() {
        assert_eq!(
            classify_local_raft_state(Some(3100), Some(3065)),
            LocalRaftState::Usable
        );
    }

    /// The pre-existing OOM case `recover_from_purge_point` was written for.
    #[test]
    fn lost_last_applied_falls_back_to_the_purge_point() {
        assert_eq!(
            classify_local_raft_state(None, Some(3065)),
            LocalRaftState::NeedsPurgePointBaseline { purge_point: 3065 }
        );
    }

    #[test]
    fn nothing_purged_is_always_usable() {
        assert_eq!(
            classify_local_raft_state(None, None),
            LocalRaftState::Usable
        );
        assert_eq!(
            classify_local_raft_state(Some(7), None),
            LocalRaftState::Usable
        );
    }

    /// A stranded node must never be silently clamped forward. Clamping is the
    /// tempting one-line fix and it fabricates applied state.
    #[test]
    fn stranded_is_never_reported_as_usable() {
        for applied in 0u64..3065 {
            let state = classify_local_raft_state(Some(applied), Some(3065));
            assert!(
                state.is_stranded(),
                "applied={applied} is behind the purge point and must be \
                 stranded, got {state:?}"
            );
        }
    }
}
