use std::fmt;

use uuid::Uuid;

/// Errors produced by ferrosa-cluster.
#[derive(Debug)]
#[non_exhaustive]
pub enum ClusterError {
    /// Not enough replicas available to satisfy consistency level.
    Unavailable {
        consistency: String,
        required: usize,
        alive: usize,
    },
    /// Replicas alive but not enough ACKed the write in time.
    WriteTimeout {
        consistency: String,
        received: usize,
        required: usize,
    },
    /// Replicas alive but not enough responded to the read in time.
    ReadTimeout {
        consistency: String,
        received: usize,
        required: usize,
        data_present: bool,
    },
    /// The coordinator or a required replica is deliberately applying
    /// backpressure and the client should slow down/retry later.
    Overloaded(String),
    /// Pair mode: primary is down, writes unavailable until operator promotes.
    PairWriteUnavailable,
    /// Operation requires primary role but this node is secondary.
    NotPrimary,
    /// This node is not the Raft leader; client should retry on the leader.
    ///
    /// `leader_id` is the openraft `NodeId` of the current leader, if known.
    NotLeader { leader_id: Option<u64> },
    /// A DDL proposal reached a Raft leader that could not get it committed
    /// within the DDL agreement bound.
    ///
    /// Distinct from [`Self::NotLeader`] (openraft named the leader) and
    /// [`Self::RaftError`] (openraft reported a fault): here the proposal is
    /// still pending on a node that believes it is the leader, and nothing is
    /// known about whether it applied anywhere. A DDL that was never confirmed
    /// must be reported as a failure the client can retry — never as done, and
    /// never by holding the client's own deadline open (forge t_e8abe81a).
    DdlProposalNotAgreed {
        /// The DDL that could not complete, named with its target (for example
        /// `DropTable ks.tbl`) so a retrying client knows which statement is
        /// unconfirmed.
        operation: String,
        /// Voters that must acknowledge the proposal (Raft quorum) in the
        /// group's membership.
        required: usize,
        /// Voters this node could confirm when the bound expired. Only the
        /// local voter holds the proposal, so this is 1.
        alive: usize,
        /// The bound that expired.
        timeout_ms: u64,
        /// How long the proposal was awaited before giving up.
        elapsed_ms: u64,
    },
    /// Attempted mode transition that is not allowed.
    ModeTransitionRejected(String),
    /// Replication to peer failed.
    ReplicationFailed(String),
    /// Peer is too far behind; full catch-up or bootstrap required.
    CatchUpRequired,
    /// Raft consensus error (fatal or unexpected).
    RaftError(String),
    /// Node has not been approved to join the cluster.
    NotApproved(Uuid),
    /// Underlying storage error.
    Storage(ferrosa_common::Error),
    /// Underlying network error.
    Net(ferrosa_net::error::NetError),
    /// Internal error.
    Internal(String),
    /// Operation is not yet implemented (Sprint 6 W6.5: cross-DC
    /// consensus path stubbed until Sprint 7's Accord wiring lands).
    /// `feature` names the missing path so logs and clients can tell
    /// "QUORUM cross-DC" apart from "EACH_QUORUM cross-DC".
    NotImplemented { feature: String },
    /// A membership change was refused because the data it moves was not
    /// streamed and verified (P0-2, P0-4). The change was not committed.
    DataMovementUnverified(String),
}

impl fmt::Display for ClusterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable {
                consistency,
                required,
                alive,
            } => {
                write!(
                    f,
                    "unavailable: CL={consistency}, required={required}, alive={alive}"
                )
            }
            Self::WriteTimeout {
                consistency,
                received,
                required,
            } => {
                write!(
                    f,
                    "write timeout: CL={consistency}, received={received}, required={required}"
                )
            }
            Self::ReadTimeout {
                consistency,
                received,
                required,
                data_present,
            } => {
                write!(
                    f,
                    "read timeout: CL={consistency}, received={received}, required={required}, data_present={data_present}"
                )
            }
            Self::Overloaded(msg) => write!(f, "overloaded: {msg}"),
            Self::PairWriteUnavailable => write!(f, "pair mode: primary unavailable"),
            Self::NotPrimary => write!(f, "this node is not the primary"),
            Self::NotLeader { leader_id } => match leader_id {
                Some(id) => write!(f, "not Raft leader; forward to node {id}"),
                None => write!(f, "not Raft leader; leader unknown"),
            },
            Self::DdlProposalNotAgreed {
                operation,
                required,
                alive,
                timeout_ms,
                elapsed_ms,
            } => write!(
                f,
                "DDL proposal not agreed: {operation} was accepted by the Raft leader but \
                 was not committed within {timeout_ms}ms (waited {elapsed_ms}ms, quorum \
                 required={required}, alive={alive}); the DDL is unconfirmed, retry it"
            ),
            Self::ModeTransitionRejected(reason) => write!(f, "mode transition rejected: {reason}"),
            Self::ReplicationFailed(reason) => write!(f, "replication failed: {reason}"),
            Self::CatchUpRequired => write!(f, "peer requires full catch-up"),
            Self::NotApproved(host_id) => write!(f, "node {host_id} not approved to join"),
            Self::RaftError(msg) => write!(f, "raft error: {msg}"),
            Self::Storage(e) => write!(f, "storage: {e}"),
            Self::Net(e) => write!(f, "net: {e}"),
            Self::Internal(msg) => write!(f, "internal: {msg}"),
            Self::NotImplemented { feature } => {
                write!(f, "not implemented: {feature}")
            }
            Self::DataMovementUnverified(msg) => {
                write!(
                    f,
                    "data movement not verified, membership change aborted: {msg}"
                )
            }
        }
    }
}

impl std::error::Error for ClusterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(e) => Some(e),
            Self::Net(e) => Some(e),
            _ => None,
        }
    }
}

impl From<ferrosa_net::error::NetError> for ClusterError {
    fn from(e: ferrosa_net::error::NetError) -> Self {
        Self::Net(e)
    }
}

impl From<ferrosa_common::Error> for ClusterError {
    fn from(e: ferrosa_common::Error) -> Self {
        Self::Storage(e)
    }
}

pub type Result<T> = std::result::Result<T, ClusterError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_messages() {
        let e = ClusterError::Unavailable {
            consistency: "QUORUM".into(),
            required: 2,
            alive: 1,
        };
        assert!(e.to_string().contains("QUORUM"));

        let e = ClusterError::PairWriteUnavailable;
        assert!(e.to_string().contains("primary"));

        // A DDL that could not be agreed names the statement, the bound it
        // waited on, and that it must be retried (never silently done).
        let e = ClusterError::DdlProposalNotAgreed {
            operation: "DropTable ks.tbl".into(),
            required: 2,
            alive: 1,
            timeout_ms: 5000,
            elapsed_ms: 5004,
        };
        let msg = e.to_string();
        assert!(msg.contains("DropTable ks.tbl"), "got: {msg}");
        assert!(msg.contains("5000"), "got: {msg}");
        assert!(msg.contains("retry"), "got: {msg}");
    }

    #[test]
    fn net_error_conversion() {
        let net_err = ferrosa_net::error::NetError::Timeout("test".into());
        let cluster_err: ClusterError = net_err.into();
        assert!(matches!(cluster_err, ClusterError::Net(_)));
    }
}
