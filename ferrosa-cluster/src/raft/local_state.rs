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

#[cfg(test)]
mod tests {
    use super::*;

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
