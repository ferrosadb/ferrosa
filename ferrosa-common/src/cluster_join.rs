//! What a peer tells a node that is trying to join, and what the node does
//! with the answer.
//!
//! # Why this exists
//!
//! A joining node used to decide its own shape from local information: its
//! peer count and its own remembered mode. Both can be wrong or absent. A node
//! reimaged clean has no memory at all, and a node whose marker file was lost
//! looks identical to a brand new one. It would then form a pair while the
//! cluster still counted it as a member and kept replicating to it -- which is
//! what node1 did on 2026-08-20.
//!
//! The authority is the CLUSTER, not the node. Raft membership says who is a
//! member; the joining node asks and is told. Local state is a cache, and the
//! protocol must be correct when that cache is empty.
//!
//! # The rules, as specified
//!
//! - A peer answers only if it can see a committed quorum. A partitioned
//!   minority stays silent rather than risk a stale answer.
//! - Removal is recorded, so a decommissioned node is told it was
//!   decommissioned rather than merely being unknown.
//! - A returning member does not bind CQL until it holds the data for the
//!   tokens it owns.
//! - Catch-up is by Raft log where the log still reaches; a full token stream
//!   only when the log was purged past the node's position.

use serde::{Deserialize, Serialize};

/// The token range a node is responsible for serving.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenRange {
    pub start: i64,
    pub end: i64,
}

/// What a peer replies when asked "am I a member of this cluster?".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipAnswer {
    /// The answering peer cannot see a committed quorum, so it does not know.
    ///
    /// Distinct from every other answer: it is the absence of one. A joiner
    /// must wait and ask again rather than treat it as "not a member", because
    /// a partitioned minority reporting "you are not a member" is exactly how a
    /// live member gets talked into forming a pair.
    NoQuorum,
    /// Committed membership has never contained this node.
    NotAMember,
    /// This node was removed deliberately.
    Decommissioned {
        /// Unix seconds, for the operator reading the refusal.
        at: u64,
        by: String,
    },
    /// This node is in committed membership and must rejoin.
    Member {
        /// Whether the cluster currently has quorum WITHOUT this node.
        cluster_degraded: bool,
        /// The tokens this node is responsible for once it is serving.
        tokens: Vec<TokenRange>,
        /// The oldest log index the cluster can still replay to this node.
        /// `None` when the log has not been purged at all.
        earliest_available_log_index: Option<u64>,
    },
}

/// How a returning member gets current before it serves anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatchUp {
    /// The cluster's log still reaches this node's position; ordinary Raft
    /// replication will carry it forward. Seconds, not minutes.
    RaftLog,
    /// The log was purged past this node's position, so there is no path from
    /// where it is to where the cluster is. Its owned tokens must be streamed.
    StreamTokens(Vec<TokenRange>),
}

/// What the joining node does with the answer it got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinAction {
    /// Ask again. The peer could not answer authoritatively.
    WaitAndRetry,
    /// No membership anywhere: this is a new node.
    FormStandalone,
    /// Removed deliberately. Do not rejoin, and say why.
    RefuseDecommissioned { at: u64, by: String },
    /// Rejoin. Reform Raft, get current, and only then serve.
    Rejoin {
        catch_up: CatchUp,
        tokens: Vec<TokenRange>,
    },
}

impl JoinAction {
    /// May the node accept CQL connections in this state?
    ///
    /// Only `FormStandalone` and a completed rejoin may. A node that is
    /// waiting, refusing, or catching up must not BIND the port at all --
    /// a refused TCP connection is something every driver already treats as
    /// "try another node", with no new client code path, and it cannot be
    /// mistaken for an empty result the way a served query can.
    #[must_use]
    pub fn may_serve_queries(&self) -> bool {
        matches!(self, Self::FormStandalone)
    }
}

/// Decide what to do with a peer's answer.
///
/// `local_last_log_index` is where this node's Raft log stops. It decides
/// between log catch-up and a full stream: if the cluster has purged past that
/// point there is no route from here to there.
#[must_use]
pub fn plan_join(answer: &MembershipAnswer, local_last_log_index: Option<u64>) -> JoinAction {
    match answer {
        MembershipAnswer::NoQuorum => JoinAction::WaitAndRetry,
        MembershipAnswer::NotAMember => JoinAction::FormStandalone,
        MembershipAnswer::Decommissioned { at, by } => JoinAction::RefuseDecommissioned {
            at: *at,
            by: by.clone(),
        },
        MembershipAnswer::Member {
            tokens,
            earliest_available_log_index,
            ..
        } => {
            let catch_up = match (local_last_log_index, earliest_available_log_index) {
                // The cluster has purged past where this node stopped, so no
                // sequence of log entries connects them.
                (Some(local), Some(earliest)) if local + 1 < *earliest => {
                    CatchUp::StreamTokens(tokens.clone())
                }
                // A member with no log at all -- reimaged clean -- cannot be
                // caught up by replay either.
                (None, _) => CatchUp::StreamTokens(tokens.clone()),
                _ => CatchUp::RaftLog,
            };
            JoinAction::Rejoin {
                catch_up,
                tokens: tokens.clone(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens() -> Vec<TokenRange> {
        vec![TokenRange {
            start: 4000,
            end: 8000,
        }]
    }

    fn member(earliest: Option<u64>) -> MembershipAnswer {
        MembershipAnswer::Member {
            cluster_degraded: true,
            tokens: tokens(),
            earliest_available_log_index: earliest,
        }
    }

    /// The case that started this: a node reimaged clean, with no local memory
    /// of ever having been a member.
    ///
    /// A marker file cannot help here -- there is no marker, because there is
    /// no disk. The peer is the only thing that knows, and it says so. Without
    /// this the node counts one peer, calls itself a pair, and serves an empty
    /// dataset for tokens the cluster believes it owns.
    #[test]
    fn a_clean_node_the_cluster_still_owns_rejoins_and_streams() {
        let action = plan_join(&member(Some(900)), None);
        assert_eq!(
            action,
            JoinAction::Rejoin {
                catch_up: CatchUp::StreamTokens(tokens()),
                tokens: tokens(),
            },
            "no local log means no replay is possible; its tokens must be streamed"
        );
        assert!(
            !action.may_serve_queries(),
            "and it must not serve until it holds that data"
        );
    }

    /// A node only briefly down catches up through the log.
    #[test]
    fn a_briefly_absent_member_catches_up_through_the_raft_log() {
        let action = plan_join(&member(Some(900)), Some(1200));
        assert_eq!(
            action,
            JoinAction::Rejoin {
                catch_up: CatchUp::RaftLog,
                tokens: tokens()
            },
            "the log still reaches this node, so replication is enough"
        );
        assert!(!action.may_serve_queries(), "still not until it is current");
    }

    /// Purged past the node's position: no sequence of entries connects them.
    #[test]
    fn a_member_behind_the_purge_point_must_stream() {
        let action = plan_join(&member(Some(900)), Some(100));
        assert_eq!(
            action,
            JoinAction::Rejoin {
                catch_up: CatchUp::StreamTokens(tokens()),
                tokens: tokens(),
            }
        );
    }

    /// Exactly at the boundary is still replayable. An off-by-one here streams
    /// gigabytes for a node that needed one entry.
    #[test]
    fn the_purge_boundary_is_inclusive() {
        assert_eq!(
            plan_join(&member(Some(900)), Some(899)),
            JoinAction::Rejoin {
                catch_up: CatchUp::RaftLog,
                tokens: tokens()
            }
        );
        assert!(matches!(
            plan_join(&member(Some(900)), Some(898)),
            JoinAction::Rejoin {
                catch_up: CatchUp::StreamTokens(_),
                ..
            }
        ));
    }

    /// A peer without quorum must not be believed, in either direction.
    ///
    /// This is the answer that prevents the whole failure: a partitioned
    /// minority saying "you are not a member" is precisely how a live member
    /// gets talked into forming a pair. Waiting is correct even though it looks
    /// like being stuck -- stuck is visible, wrong is not.
    #[test]
    fn a_peer_without_quorum_is_never_treated_as_an_answer() {
        let action = plan_join(&MembershipAnswer::NoQuorum, Some(1200));
        assert_eq!(action, JoinAction::WaitAndRetry);
        assert!(!action.may_serve_queries());
        assert_ne!(
            action,
            JoinAction::FormStandalone,
            "'I cannot tell you' must never collapse into 'you are not a member'"
        );
    }

    /// A decommissioned node is refused, and told why.
    #[test]
    fn a_decommissioned_node_refuses_to_rejoin_and_says_why() {
        let action = plan_join(
            &MembershipAnswer::Decommissioned {
                at: 1_787_000_000,
                by: "bkearns".into(),
            },
            Some(1200),
        );
        assert_eq!(
            action,
            JoinAction::RefuseDecommissioned {
                at: 1_787_000_000,
                by: "bkearns".into()
            }
        );
        assert!(
            !action.may_serve_queries(),
            "a removed node must not serve; it holds data it no longer owns"
        );
    }

    /// A genuinely new node still forms standalone and serves. The fix must not
    /// make first boot wait for a cluster that does not exist.
    #[test]
    fn an_unknown_node_forms_standalone_and_may_serve() {
        let action = plan_join(&MembershipAnswer::NotAMember, None);
        assert_eq!(action, JoinAction::FormStandalone);
        assert!(action.may_serve_queries());
    }

    /// Serving is the exception, not the default.
    #[test]
    fn only_a_standalone_node_may_serve_before_rejoining() {
        let states = [
            plan_join(&MembershipAnswer::NoQuorum, None),
            plan_join(&member(Some(1)), None),
            plan_join(&member(Some(1)), Some(1000)),
            plan_join(
                &MembershipAnswer::Decommissioned {
                    at: 0,
                    by: "x".into(),
                },
                None,
            ),
        ];
        for state in states {
            assert!(
                !state.may_serve_queries(),
                "{state:?} must not bind CQL: a refused connection is something every \
driver already handles, and an empty answer is not"
            );
        }
    }
}
