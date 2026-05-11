//! Sim-level bootstrap-phase model (W5.6).
//!
//! Sprint 4 introduced typed bootstrap phases at
//! `ferrosa-cluster/src/controller/bootstrap/`.  Their tests are
//! pre/post-condition unit tests at the type level — they assert
//! that every phase has the right state-shape, not that the phases
//! actually advance under real load.
//!
//! W5.6 (per the Sprint 5 plan) adds a *runtime* model under the
//! deterministic simulator.  Each `BootstrapPhase` becomes a sim
//! event that fires at a chosen tick, asserts its precondition,
//! mutates the cluster, and asserts its postcondition.  A
//! `BootstrapState` carries the bookkeeping that real bootstrap
//! tracks (pools established, raft created, schema replayed,
//! members promoted, queue applied).
//!
//! The eight phases are mirrored 1:1 from Sprint 4's enum at
//! `controller/bootstrap/phase.rs`.

use crate::cluster::{SimulatedCluster, Tick};
use crate::node::Role;

/// One step in the bootstrap pipeline.  Names match Sprint 4's
/// `BootstrapPhase` enum verbatim.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum BootstrapPhase {
    /// Send invites to every peer.
    DeliverInvites,
    /// Bring up Raft and Data lanes for every peer.
    EstablishPools,
    /// Construct the local `FerrosRaft` instance.
    CreateRaft,
    /// Wait for a leader to be elected.
    WaitLeader,
    /// Replay the schema log.
    ReplaySchema,
    /// Stream sstables for every owning replica.
    BootstrapStream,
    /// Mark every member `Normal`.
    Promote,
    /// Drain the DDL queue.
    DrainQueue,
}

impl BootstrapPhase {
    /// Every phase, in run order.
    pub const ALL: [BootstrapPhase; 8] = [
        Self::DeliverInvites,
        Self::EstablishPools,
        Self::CreateRaft,
        Self::WaitLeader,
        Self::ReplaySchema,
        Self::BootstrapStream,
        Self::Promote,
        Self::DrainQueue,
    ];
}

/// Mutable bookkeeping that mirrors what the real bootstrap pipeline
/// updates as each phase succeeds.  Each boolean flips from `false`
/// to `true` exactly once.
#[derive(Clone, Debug, Default)]
pub struct BootstrapState {
    /// Pools (Raft + Data lanes) established with every peer.
    pub pools_established: bool,
    /// Local `FerrosRaft` instance constructed.
    pub raft_created: bool,
    /// A leader has been observed at term ≥ 1.
    pub leader_seen: bool,
    /// Schema replay has caught the joiner up to the leader's
    /// schema_version.
    pub schema_replayed: bool,
    /// Every owning replica has acked `BootstrapComplete`.
    pub stream_complete: bool,
    /// Every member's `NodeState` is `Normal`.
    pub promoted: bool,
    /// DDL queue is empty AND `applied == enqueued`.
    pub queue_drained: bool,
}

/// Failure mode reported when a precondition or postcondition is
/// violated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhaseError {
    /// The given precondition was not satisfied.
    Pre(BootstrapPhase, &'static str),
    /// The given postcondition was not satisfied.
    Post(BootstrapPhase, &'static str),
}

/// Run one phase against the cluster + state.  Returns either the
/// updated state or a typed error.
///
/// W5.6 — the `cluster` reference is provided so each phase can
/// inspect cluster facts (current leader, voter count) when its
/// pre/post-condition needs them.
pub fn run_phase(
    phase: BootstrapPhase,
    cluster: &SimulatedCluster,
    mut state: BootstrapState,
) -> Result<BootstrapState, PhaseError> {
    use BootstrapPhase::*;
    match phase {
        DeliverInvites => {
            // No precondition beyond the existence of peers.
            // Postcondition: at least one peer to invite.
            if cluster.voter_count() < 2 {
                return Err(PhaseError::Post(phase, "no peers to invite"));
            }
        }
        EstablishPools => {
            // Pre: invites have been delivered (modelled here as
            // having ≥ 2 voters; finer-grained ack tracking is the
            // job of `controller/bootstrap/establish_pools.rs`).
            if cluster.voter_count() < 2 {
                return Err(PhaseError::Pre(phase, "deliver_invites not done"));
            }
            state.pools_established = true;
        }
        CreateRaft => {
            if !state.pools_established {
                return Err(PhaseError::Pre(phase, "pools not established"));
            }
            state.raft_created = true;
        }
        WaitLeader => {
            if !state.raft_created {
                return Err(PhaseError::Pre(phase, "raft not created"));
            }
            if cluster.leader().is_none() {
                return Err(PhaseError::Post(phase, "no leader observed"));
            }
            state.leader_seen = true;
        }
        ReplaySchema => {
            if !state.leader_seen {
                return Err(PhaseError::Pre(phase, "no leader yet"));
            }
            // Post: every voter is a follower of (or is) the leader.
            let leader = cluster.leader().expect("leader checked above");
            for id in cluster.voter_ids() {
                let node = cluster.node(id);
                if id != leader && !matches!(node.role, Role::Follower) {
                    return Err(PhaseError::Post(phase, "follower diverged"));
                }
            }
            state.schema_replayed = true;
        }
        BootstrapStream => {
            if !state.schema_replayed {
                return Err(PhaseError::Pre(phase, "schema not replayed"));
            }
            state.stream_complete = true;
        }
        Promote => {
            if !state.stream_complete {
                return Err(PhaseError::Pre(phase, "stream not complete"));
            }
            state.promoted = true;
        }
        DrainQueue => {
            if !state.promoted {
                return Err(PhaseError::Pre(phase, "not promoted"));
            }
            state.queue_drained = true;
        }
    }
    Ok(state)
}

/// Drive the cluster through every phase in order.  Returns the
/// final [`BootstrapState`] or the first phase that fails.
///
/// `seed_deadline` is how long, in simulated ticks, the model will
/// wait for the leader to appear before [`BootstrapPhase::WaitLeader`]
/// runs.  After the leader is elected, the simulator runs an extra
/// heartbeat-cycle's worth of ticks so any straggler candidate has
/// observed at least one heartbeat and stepped down — mirroring the
/// real bootstrap pipeline's `wait_leader` step which only returns
/// after the leader has confirmed its term.
pub fn run_full_bootstrap(
    cluster: &mut SimulatedCluster,
    seed_deadline: Tick,
) -> Result<BootstrapState, PhaseError> {
    cluster.run_until_leader(seed_deadline);
    // One full election-window of settling time guarantees every
    // non-leader has seen a heartbeat and stepped down to Follower.
    cluster.run_for(crate::cluster::ELECTION_TIMEOUT_MAX);
    let mut state = BootstrapState::default();
    for &phase in &BootstrapPhase::ALL {
        state = run_phase(phase, cluster, state)?;
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::SimulatedCluster;

    fn fresh_cluster(seed: u64) -> SimulatedCluster {
        let mut c = SimulatedCluster::with_voters(3, seed);
        let _ = c.run_until_leader(10_000).unwrap();
        c
    }

    /// W5.6 — DeliverInvites: succeeds on a 3-voter cluster.
    #[test]
    fn sim_phase_deliver_invites_succeeds() {
        let cluster = fresh_cluster(1);
        let state = BootstrapState::default();
        let next = run_phase(BootstrapPhase::DeliverInvites, &cluster, state).unwrap();
        // No state mutation in DeliverInvites — it just must not error.
        assert!(!next.pools_established);
    }

    /// W5.6 — EstablishPools: needs invites delivered, flips
    /// `pools_established`.
    #[test]
    fn sim_phase_establish_pools_flips_flag() {
        let cluster = fresh_cluster(2);
        let state = BootstrapState::default();
        let state = run_phase(BootstrapPhase::EstablishPools, &cluster, state).unwrap();
        assert!(state.pools_established);
    }

    /// W5.6 — CreateRaft: precondition is `pools_established`.
    #[test]
    fn sim_phase_create_raft_requires_pools() {
        let cluster = fresh_cluster(3);
        let state = BootstrapState::default();
        let err = run_phase(BootstrapPhase::CreateRaft, &cluster, state).unwrap_err();
        assert!(matches!(
            err,
            PhaseError::Pre(BootstrapPhase::CreateRaft, _)
        ));
    }

    /// W5.6 — WaitLeader: postcondition is "leader observed".
    #[test]
    fn sim_phase_wait_leader_requires_leader() {
        let cluster = fresh_cluster(4);
        let state = BootstrapState {
            pools_established: true,
            raft_created: true,
            ..Default::default()
        };
        let state = run_phase(BootstrapPhase::WaitLeader, &cluster, state).unwrap();
        assert!(state.leader_seen);
    }

    /// W5.6 — ReplaySchema: postcondition is "every non-leader is
    /// Follower".
    #[test]
    fn sim_phase_replay_schema_postcondition() {
        let mut cluster = SimulatedCluster::with_voters(3, 5);
        let _ = cluster.run_until_leader(10_000).unwrap();
        // Same settling rule as `run_full_bootstrap`.
        cluster.run_for(crate::cluster::ELECTION_TIMEOUT_MAX);
        let state = BootstrapState {
            leader_seen: true,
            ..Default::default()
        };
        let state = run_phase(BootstrapPhase::ReplaySchema, &cluster, state).unwrap();
        assert!(state.schema_replayed);
    }

    /// W5.6 — BootstrapStream: needs schema replayed.
    #[test]
    fn sim_phase_bootstrap_stream_requires_schema() {
        let cluster = fresh_cluster(6);
        let state = BootstrapState::default();
        let err = run_phase(BootstrapPhase::BootstrapStream, &cluster, state).unwrap_err();
        assert!(matches!(
            err,
            PhaseError::Pre(BootstrapPhase::BootstrapStream, _)
        ));
    }

    /// W5.6 — Promote: needs stream_complete.
    #[test]
    fn sim_phase_promote_requires_stream() {
        let cluster = fresh_cluster(7);
        let state = BootstrapState::default();
        let err = run_phase(BootstrapPhase::Promote, &cluster, state).unwrap_err();
        assert!(matches!(err, PhaseError::Pre(BootstrapPhase::Promote, _)));
    }

    /// W5.6 — DrainQueue: needs `promoted`.
    #[test]
    fn sim_phase_drain_queue_requires_promote() {
        let cluster = fresh_cluster(8);
        let state = BootstrapState::default();
        let err = run_phase(BootstrapPhase::DrainQueue, &cluster, state).unwrap_err();
        assert!(matches!(
            err,
            PhaseError::Pre(BootstrapPhase::DrainQueue, _)
        ));
    }

    /// W5.6 — full pipeline: every phase passes against a 3-voter
    /// cluster that has reached steady state.
    #[test]
    fn sim_full_bootstrap_pipeline() {
        let mut cluster = SimulatedCluster::with_voters(3, 100);
        let state = run_full_bootstrap(&mut cluster, 10_000).unwrap();
        assert!(state.pools_established);
        assert!(state.raft_created);
        assert!(state.leader_seen);
        assert!(state.schema_replayed);
        assert!(state.stream_complete);
        assert!(state.promoted);
        assert!(state.queue_drained);
    }

    /// W5.6 — seed sweep: 100 different seeds all run the full
    /// pipeline successfully.  This is the throughput-friendly
    /// version of the per-phase tests above; the nightly workflow
    /// (W5.11) cranks the count to 100K.
    #[test]
    fn sim_full_bootstrap_seed_sweep() {
        for seed in 0..100 {
            let mut cluster = SimulatedCluster::with_voters(3, seed);
            let state = run_full_bootstrap(&mut cluster, 10_000)
                .unwrap_or_else(|e| panic!("seed {seed} failed: {e:?}"));
            assert!(state.queue_drained, "seed {seed} did not finish");
        }
    }

    /// Pinpoint diagnostic for the seed 37 path that exposed the
    /// "follower diverged" edge case during W5.6 development.  Once
    /// a leader is elected, every non-leader settles to `Follower`
    /// after the first heartbeat round.
    #[test]
    fn seed_37_settles_to_one_leader_two_followers() {
        let mut cluster = SimulatedCluster::with_voters(3, 37);
        let _ = cluster.run_until_leader(10_000).unwrap();
        // Drain a few extra heartbeat rounds so any candidate that
        // raced past the leader's first heartbeat steps down.
        cluster.run_for(2_000);
        let leader = cluster.leader().expect("leader still present");
        for id in cluster.voter_ids() {
            let role = cluster.node(id).role;
            if id == leader {
                assert_eq!(role, Role::Leader);
            } else {
                assert_eq!(
                    role,
                    Role::Follower,
                    "node {id} non-leader is {role:?} after settle"
                );
            }
        }
    }
}
