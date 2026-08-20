//! Cluster formation lifecycle.
//!
//! The state machine moved to `ferrosa-common` so `ferrosa-sim` can share it
//! rather than keep a hand-synchronised mirror. Re-exported here because the
//! path `crate::mode::DeploymentMode` is used widely across this crate and in
//! external tests.
//!
//! Why the move: `ferrosa-sim` carried its own copy with the comment "kept in
//! lock-step manually". They had drifted -- the mirror lacked DegradedPair and
//! DegradedCluster entirely, so the simulator could not represent a degraded
//! cluster at all, and its `from_peer_count` mapped one peer to Pair with no
//! way to express the rule that a Raft cluster never becomes a pair again.

pub use ferrosa_common::deployment_mode::DeploymentMode;
