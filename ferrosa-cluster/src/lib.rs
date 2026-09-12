//! Ferrosa cluster coordination, replication, consistency, and health gates.
//! Correctness: exported controllers fail closed when consensus is unavailable.
//! Last revised: 2026-08-27
//! Last changed: Exported bounded consensus health supervision.

pub mod accord;
pub mod config;
pub mod consensus_health;
pub mod consistency;
pub mod controller;
pub mod coordinator;
pub mod ddl_path;
pub mod error;
pub mod hints;
pub mod index_coordination;
pub mod membership;
pub mod mode;
pub mod pair;
pub mod raft;
pub mod raft_forward;
pub mod rebalance;
pub mod repair;
pub mod ring;
pub mod state;
pub mod streaming;
pub mod system_table_loader;
pub mod system_table_writer;
pub mod telemetry;
pub mod write_path;

pub use config::{ClusterConfig, PerDcOverride};
pub use consensus_health::{ConsensusFailure, ConsensusHealth, CONSENSUS_DIAGNOSTIC_CAPACITY};
pub use consistency::ConsistencyLevel;
pub use controller::{
    bootstrap_silent_failure_counts, cluster_rejoin_attempts_total, cluster_rejoin_failures_total,
    ClusterStateHolder, ModeController, ModeControllerHandles, CLUSTER_REJOIN_ATTEMPTS_TOTAL,
    CLUSTER_REJOIN_FAILURES_TOTAL,
};
pub use coordinator::batch::{
    BatchlogDeleteHandler, BatchlogReplayHandler, BatchlogReplayTask, BatchlogWriteHandler,
};
pub use coordinator::{
    ClusterCoordinator, MutationForwardHandler, RepairWriteHandler, TruncateForwardHandler,
};
pub use ddl_path::DdlPath;
pub use error::{ClusterError, Result};
pub use mode::DeploymentMode;
pub use pair::{PairCoordinator, PairNode, PairRole};
pub use repair::{
    AutoRepairConfig, AutoRepairScheduler, InMemoryRepairStore, LocalRepairExecutor,
    RemoteRepairStore, RepairApplyHandler, RepairContext, RepairCoordinator, RepairFetchHandler,
    RepairMerkleHandler, RepairStore, SessionExecutor, SessionStats, StorageEngineRepairStore,
};
pub use state::{PairClusterState, RaftClusterState, SingleNodeClusterState};
pub use write_path::WritePath;
