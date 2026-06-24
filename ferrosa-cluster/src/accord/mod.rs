//! Accord consensus protocol support for Ferrosa.
//!
//! This module contains the deterministic test harness ([`TestCluster`]) for
//! protocol-level testing of the Accord (EPaxos-family) consensus protocol.

pub mod apply;
pub mod chaos_minority_kill;
pub mod clock;
pub mod clock_validation;
pub mod coordinator;
pub mod cross_dc_adapter;
pub mod cross_shard;
pub mod ddl_drain;
pub mod dep_wait;
#[cfg(test)]
mod driver_cross_shard_e2e;
pub mod durability;
pub mod electorate;
pub mod epoch;
pub mod epoch_drain;
/// RPC handlers for inbound Accord consensus messages.
pub mod handlers;
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
pub mod shard_quorum;
pub mod state_machine;
pub mod test_cluster;
pub mod transaction_commit;
pub mod transport;
pub mod two_phase_ddl;
pub mod uda_integration;
pub(crate) mod wire;

pub use apply::{
    ApplyError, ApplyMutation, DepWaitApplier, EngineStorageApplier, EngineStorageReader,
    NoopStorageApplier, RowReadError, StorageApplier, StorageReader,
};
pub use clock::{ClockError, ClockValidator};
pub use clock_validation::{
    validate_timestamp_drift, ClockDriftRejection, DEFAULT_MAX_CLOCK_DRIFT_NS,
};
pub use coordinator::{
    fast_quorum_size, slow_quorum_size, AccordCoordinator, AccordCoordinatorDriver,
    AccordDriverError, ConditionGate, CoordinatorDecision, CoordinatorPhase,
};
pub use cross_dc_adapter::{
    cross_dc_vote_commit_count, CrossDcAccordAdapter, CROSS_DC_VOTE_COMMITS,
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
pub use recovery::{
    AccordNodeState, AccordPhase, AccordTxn, InflightResolution, NodeId, NodeRecoveryCoordinator,
    RecoverOKResponse, RecoveryCoordinator, RecoveryDecision,
};
pub use reorder_buffer::ReorderBuffer;
pub use state_machine::{AccordStateMachine, SmResponse};
pub use test_cluster::{TestCluster, TestMessage, TestMessagePayload, TestReplica};
pub use two_phase_ddl::{DdlMarker, DdlOperation, DdlPhase, TwoPhaseDdlError, TwoPhaseDdlManager};
pub use wire::ReadPredicate;
