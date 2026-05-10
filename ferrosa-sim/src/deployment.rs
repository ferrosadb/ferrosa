//! Mirror of `ferrosa-cluster::mode::DeploymentMode`.
//!
//! Owned here so `ferrosa-sim` can stay free of ferrosa-cluster's
//! storage stack.  The two enums are kept in lock-step manually; a
//! Sprint 5 follow-up could lift this into `ferrosa-common`, but
//! today nothing else in the workspace needs it.

use serde::{Deserialize, Serialize};

/// Cluster lifecycle mode mirrored from
/// `ferrosa_cluster::mode::DeploymentMode`.
///
/// Only the fields the simulator transitions through are modelled;
/// the full state machine lives in `ferrosa-cluster`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeploymentMode {
    /// Single-node, no peers.
    Standalone,
    /// Two-node, mesh forming.
    Pair,
    /// 3rd peer seen, awaiting Raft init.
    Forming,
    /// Steady-state cluster with a leader.
    Cluster,
}

impl DeploymentMode {
    /// Infer the mode from peer count (excluding self).
    pub fn from_peer_count(count: usize) -> Self {
        match count {
            0 => Self::Standalone,
            1 => Self::Pair,
            _ => Self::Cluster,
        }
    }
}
