use std::fmt;

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
    /// Pair mode: primary is down, writes unavailable until operator promotes.
    PairWriteUnavailable,
    /// Operation requires primary role but this node is secondary.
    NotPrimary,
    /// Attempted mode transition that is not allowed.
    ModeTransitionRejected(String),
    /// Replication to peer failed.
    ReplicationFailed(String),
    /// Peer is too far behind; full catch-up or bootstrap required.
    CatchUpRequired,
    /// Underlying storage error.
    Storage(ferrosa_common::Error),
    /// Underlying network error.
    Net(ferrosa_net::error::NetError),
    /// Internal error.
    Internal(String),
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
            Self::PairWriteUnavailable => write!(f, "pair mode: primary unavailable"),
            Self::NotPrimary => write!(f, "this node is not the primary"),
            Self::ModeTransitionRejected(reason) => write!(f, "mode transition rejected: {reason}"),
            Self::ReplicationFailed(reason) => write!(f, "replication failed: {reason}"),
            Self::CatchUpRequired => write!(f, "peer requires full catch-up"),
            Self::Storage(e) => write!(f, "storage: {e}"),
            Self::Net(e) => write!(f, "net: {e}"),
            Self::Internal(msg) => write!(f, "internal: {msg}"),
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
    }

    #[test]
    fn net_error_conversion() {
        let net_err = ferrosa_net::error::NetError::Timeout("test".into());
        let cluster_err: ClusterError = net_err.into();
        assert!(matches!(cluster_err, ClusterError::Net(_)));
    }
}
