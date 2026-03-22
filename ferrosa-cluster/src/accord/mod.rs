//! Accord consensus protocol support for Ferrosa.
//!
//! This module contains the deterministic test harness ([`TestCluster`]) for
//! protocol-level testing of the Accord (EPaxos-family) consensus protocol.

pub mod clock_validation;
pub mod cross_shard;
pub mod ddl_drain;
pub mod dep_wait;
pub mod proptests;
pub mod recovery;
pub mod recovery_scenarios;
pub mod reorder_buffer;
pub mod state_machine;
pub mod test_cluster;

pub use clock_validation::{
    validate_timestamp_drift, ClockDriftRejection, DEFAULT_MAX_CLOCK_DRIFT_NS,
};
pub use cross_shard::{CrossShardCoordinator, CrossShardOutcome, ShardId, ShardResult};
pub use ddl_drain::{DdlDrainGuard, DrainError};
pub use dep_wait::{DepWaitError, DepWaitGraph};
pub use recovery::{RecoverOKResponse, RecoveryCoordinator, RecoveryDecision};
pub use reorder_buffer::ReorderBuffer;
pub use state_machine::{AccordStateMachine, SmResponse};
pub use test_cluster::{TestCluster, TestMessage, TestMessagePayload, TestReplica};
