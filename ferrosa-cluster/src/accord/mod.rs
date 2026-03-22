//! Accord consensus protocol support for Ferrosa.
//!
//! This module contains the deterministic test harness ([`TestCluster`]) for
//! protocol-level testing of the Accord (EPaxos-family) consensus protocol.

pub mod chaos_minority_kill;
pub mod clock_validation;
pub mod coordinator;
pub mod cross_shard;
pub mod ddl_drain;
pub mod dep_wait;
pub mod durability;
pub mod electorate;
pub mod epoch;
pub mod epoch_drain;
mod jepsen_bank;
pub mod jepsen_nemesis;
pub mod leaseholder;
pub mod linearizable_read;
pub mod metrics;
pub mod perf;
pub mod perf_regression;
pub mod proptests;
pub mod recovery;
pub mod recovery_scenarios;
pub mod reorder_buffer;
pub mod state_machine;
pub mod test_cluster;
pub mod two_phase_ddl;
pub mod uda_integration;

pub use clock_validation::{
    validate_timestamp_drift, ClockDriftRejection, DEFAULT_MAX_CLOCK_DRIFT_NS,
};
pub use coordinator::{
    fast_quorum_size, slow_quorum_size, AccordCoordinator, CoordinatorDecision, CoordinatorPhase,
};
pub use cross_shard::{CrossShardCoordinator, CrossShardOutcome, ShardId, ShardResult};
pub use ddl_drain::{DdlDrainGuard, DrainError};
pub use dep_wait::{DepWaitError, DepWaitGraph};
pub use durability::{DurabilityHealth, DurabilityService, ExclusiveSyncPoint, ShardProgress};
pub use electorate::{Electorate, ElectorateError, JoinGate, MemberStatus, ProtocolHistory};
pub use epoch::{EpochDecision, EpochMessage, EpochTracker, MessageKind, QuorumEpochCheck};
pub use epoch_drain::{DrainCheckResult, EpochDrain, TxnDrainStatus};
pub use leaseholder::{LeaseAssignment, LeaseError, LeaseholderManager};
pub use linearizable_read::{LinearizableReadManager, ReadError, ReadResult};
pub use metrics::AccordMetrics;
pub use recovery::{RecoverOKResponse, RecoveryCoordinator, RecoveryDecision};
pub use reorder_buffer::ReorderBuffer;
pub use state_machine::{AccordStateMachine, SmResponse};
pub use test_cluster::{TestCluster, TestMessage, TestMessagePayload, TestReplica};
pub use two_phase_ddl::{DdlMarker, DdlOperation, DdlPhase, TwoPhaseDdlError, TwoPhaseDdlManager};
